mod checkpoint_publication {
    use super::*;

    use super::super::{TableActorHandle, TableActorPreparedPersist};

    struct PreparedFixture {
        registry: TableActorRegistry,
        actor: TableActorHandle,
        prepared: TableActorPreparedPersist,
        submissions: mpsc::UnboundedReceiver<FakeSubmission>,
        writer: ActiveSegmentWriter,
    }

    fn live_publication(
        writer: &ActiveSegmentWriter,
        rows: usize,
    ) -> TableActorLiveSegmentsPublication {
        let descriptor = writer.active_descriptor();
        TableActorLiveSegmentsPublication::new(
            table_id(10),
            vec![TableActorLiveSegmentPublication::new(
                TableActorLiveSegmentRole::Active,
                SourceSegmentIdentity {
                    segment_id: descriptor.segment_id(),
                    generation: descriptor.generation(),
                    writer_epoch: descriptor.writer_epoch(),
                },
                1,
                OffsetRange::new(LogOffset::new(0), LogOffset::new(rows as u64 - 1)).unwrap(),
                descriptor,
                0,
                rows,
            )],
        )
    }

    async fn prepared_fixture() -> PreparedFixture {
        let (submitter, mut submissions) = fake_submitter();
        let registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10]),
            submitter,
        )
        .unwrap();
        let actor = registry.handle().table(table_id(10)).unwrap();
        let segment_schema =
            compile_schema(&[ColumnSpec::new("price", ColumnType::Int64)]).unwrap();
        let layout = LayoutPlan::for_schema(&segment_schema, 8).unwrap();
        let mut writer =
            ActiveSegmentWriter::new_for_runtime_with_epoch(segment_schema, layout, 11, 2, 7, 1)
                .unwrap();
        for price in [11_i64, 22] {
            writer.begin_row().unwrap();
            writer.write_i64("price", price).unwrap();
            writer.commit_row().unwrap();
        }
        actor
            .publish_live_segments(live_publication(&writer, 2))
            .await
            .unwrap();
        let preparing_actor = actor.clone();
        let preparation = tokio::spawn(async move {
            preparing_actor
                .prepare_persist(persist_request(400, 10, 1, 1, 7, 0, vec![11]))
                .await
        });
        let submission = submissions.recv().await.unwrap();
        assert_eq!(submission.request_id, request_id(400));
        submission
            .response
            .send(Ok(successful_receipt(
                400,
                10,
                1,
                OffsetWatermark::Known(LogOffset::new(0)),
            )))
            .unwrap();
        let prepared = preparation.await.unwrap().unwrap();
        PreparedFixture {
            registry,
            actor,
            prepared,
            submissions,
            writer,
        }
    }

    fn live_prices(snapshot: &super::super::TableActorSnapshot) -> Vec<i64> {
        snapshot
            .live_segments
            .iter()
            .flat_map(|segment| {
                let batch = segment.span().as_record_batch().unwrap();
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn prepared_receipt_keeps_old_snapshot_and_accepts_continuous_live_writes() {
        let PreparedFixture {
            registry,
            actor,
            prepared,
            mut writer,
            ..
        } = prepared_fixture().await;
        let before = actor.snapshot().await.unwrap();
        assert_eq!(before.persisted_snapshot_version, 1);
        assert_eq!(before.committed_log_offset, OffsetWatermark::Empty);
        assert_eq!(live_prices(&before), vec![11, 22]);
        assert_eq!(
            before.persistence_status,
            TablePersistenceStatus::InFlight {
                request_id: request_id(400)
            }
        );

        writer.begin_row().unwrap();
        writer.write_i64("price", 33).unwrap();
        writer.commit_row().unwrap();
        actor
            .publish_live_segments(live_publication(&writer, 3))
            .await
            .unwrap();
        let during = actor.snapshot().await.unwrap();
        assert_eq!(during.persisted_snapshot_version, 1);
        assert_eq!(live_prices(&during), vec![11, 22, 33]);
        assert_eq!(
            during.max_log_offset,
            OffsetWatermark::Known(LogOffset::new(2))
        );

        let publication = tokio::task::spawn_blocking(move || prepared.publish_blocking())
            .await
            .unwrap()
            .unwrap();
        let after = actor.snapshot().await.unwrap();
        assert_eq!(after.persisted_snapshot_version, 2);
        assert_eq!(
            after.committed_log_offset,
            OffsetWatermark::Known(LogOffset::new(0))
        );
        assert_eq!(live_prices(&after), vec![22, 33]);
        assert_eq!(after.live_segments[0].offsets().first(), LogOffset::new(1));
        assert_eq!(after.live_segments[0].offsets().last(), LogOffset::new(2));
        assert_eq!(
            live_prices(&before),
            vec![11, 22],
            "already pinned views retain their original prefix"
        );
        drop(publication);
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn pending_publication_rejects_persist_epoch_and_retirement_without_losing_receipt() {
        let PreparedFixture {
            registry,
            actor,
            prepared,
            mut submissions,
            ..
        } = prepared_fixture().await;
        let second = actor
            .try_persist(persist_request(401, 10, 1, 1, 7, 0, vec![11]))
            .unwrap_err();
        assert_eq!(second.code(), ErrorCode::ResourceExhausted);
        assert!(second.retryable());
        assert_eq!(
            second.message(),
            "native table already has one admitted persist operation"
        );
        let fencing = actor.update_writer_epoch(8).await.unwrap_err();
        assert_eq!(fencing.code(), ErrorCode::ResourceExhausted);
        assert!(fencing.retryable());
        let transition = registry
            .registrar()
            .prepare_retirement(table_id(10))
            .await
            .err()
            .unwrap();
        assert_eq!(transition.code(), ErrorCode::ResourceExhausted);
        assert!(transition.retryable());
        assert_eq!(
            transition.message(),
            "table actor persistence must finish before a table transition"
        );
        assert_eq!(actor.snapshot().await.unwrap().writer_epoch, 7);
        assert!(submissions.try_recv().is_err());

        let publication = tokio::task::spawn_blocking(move || prepared.publish_blocking())
            .await
            .unwrap()
            .unwrap();
        let while_reclaiming = actor
            .try_persist(persist_request(402, 10, 2, 1, 7, 1, vec![22]))
            .unwrap_err();
        assert_eq!(while_reclaiming.code(), ErrorCode::ResourceExhausted);
        assert!(registry
            .registrar()
            .prepare_retirement(table_id(10))
            .await
            .is_err());
        let after = actor.snapshot().await.unwrap();
        assert_eq!(after.persisted_snapshot_version, 2);
        assert_eq!(live_prices(&after), vec![22]);
        drop(publication);

        actor.update_writer_epoch(8).await.unwrap();
        let retirement = registry
            .registrar()
            .prepare_retirement(table_id(10))
            .await
            .unwrap();
        retirement
            .retire(zippy_types::table::TableRetirementReason::Dropped)
            .await
            .unwrap();
        assert!(actor.snapshot().await.is_err());
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_keeps_private_publication_available_after_ordinary_admission_closes() {
        let PreparedFixture {
            mut registry,
            actor,
            prepared,
            ..
        } = prepared_fixture().await;
        registry.close_admission();
        assert_eq!(
            actor.snapshot().await.unwrap_err().code(),
            ErrorCode::Unavailable
        );
        let mut draining = tokio::spawn(registry.drain());
        let publication = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || prepared.publish_blocking()),
        )
        .await
        .expect("publication does not depend on closed mailbox admission")
        .unwrap()
        .unwrap();
        drop(publication);
        tokio::time::timeout(Duration::from_secs(2), &mut draining)
            .await
            .expect("shutdown joins after its reserved publication completes")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn abandoned_prepared_owner_fails_closed_and_reports_the_committed_request() {
        let PreparedFixture {
            registry,
            actor,
            prepared,
            ..
        } = prepared_fixture().await;
        let handle = registry.handle();
        let mut monitor = registry.failure_monitor();
        drop(prepared);
        let failure = tokio::time::timeout(Duration::from_secs(2), monitor.wait_for_failure())
            .await
            .expect("lost publication owner fails closed");
        assert_eq!(failure.code(), ErrorCode::Unavailable);
        assert_eq!(failure.request_id(), Some(request_id(400)));
        assert!(!handle.is_accepting());
        assert_eq!(
            actor.snapshot().await.unwrap_err().code(),
            ErrorCode::Unavailable
        );
        let failure = registry.drain().await.unwrap_err();
        assert_eq!(failure.request_id(), Some(request_id(400)));
    }

    #[tokio::test]
    async fn rejected_preparation_releases_single_flight_and_keeps_original_snapshot() {
        let (submitter, mut submissions) = fake_submitter();
        let registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10]),
            submitter,
        )
        .unwrap();
        let actor = registry.handle().table(table_id(10)).unwrap();
        let preparing_actor = actor.clone();
        let preparation = tokio::spawn(async move {
            preparing_actor
                .prepare_persist(persist_request(410, 10, 1, 1, 7, 0, vec![11]))
                .await
        });
        submissions
            .recv()
            .await
            .unwrap()
            .response
            .send(Err(PersistCompletionFailure::Rejected(ServerError::new(
                ErrorCode::Conflict,
                "definitive checkpoint rejection",
            ))))
            .unwrap();
        let rejected = preparation.await.unwrap().err().unwrap();
        assert_eq!(rejected.code(), ErrorCode::Conflict);
        let unchanged = actor.snapshot().await.unwrap();
        assert_eq!(unchanged.persisted_snapshot_version, 1);
        assert_eq!(unchanged.persistence_status, TablePersistenceStatus::Idle);
        assert!(registry.handle().is_accepting());
        let retry = actor
            .try_persist(persist_request(411, 10, 1, 1, 7, 0, vec![11]))
            .unwrap();
        submissions
            .recv()
            .await
            .unwrap()
            .response
            .send(Ok(successful_receipt(
                411,
                10,
                1,
                OffsetWatermark::Known(LogOffset::new(0)),
            )))
            .unwrap();
        retry.wait().await.unwrap();
        assert_eq!(
            actor.snapshot().await.unwrap().persisted_snapshot_version,
            2
        );
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn fatal_preparation_preserves_terminal_error_instead_of_creating_an_owner() {
        let (submitter, mut submissions) = fake_submitter();
        let registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10]),
            submitter,
        )
        .unwrap();
        let actor = registry.handle().table(table_id(10)).unwrap();
        let mut monitor = registry.failure_monitor();
        let preparing_actor = actor.clone();
        let preparation = tokio::spawn(async move {
            preparing_actor
                .prepare_persist(persist_request(420, 10, 1, 1, 7, 0, vec![11]))
                .await
        });
        submissions
            .recv()
            .await
            .unwrap()
            .response
            .send(Err(PersistCompletionFailure::Fatal(ServerError::new(
                ErrorCode::DataCorruption,
                "durable checkpoint receipt is corrupt",
            ))))
            .unwrap();
        assert_eq!(
            preparation.await.unwrap().err().unwrap().code(),
            ErrorCode::DataCorruption
        );
        let failure = tokio::time::timeout(Duration::from_secs(2), monitor.wait_for_failure())
            .await
            .unwrap();
        assert_eq!(failure.code(), ErrorCode::DataCorruption);
        assert_eq!(failure.request_id(), Some(request_id(420)));
        assert!(!registry.handle().is_accepting());
        assert_eq!(
            registry.drain().await.unwrap_err().code(),
            ErrorCode::DataCorruption
        );
    }
}
