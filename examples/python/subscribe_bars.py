import pyarrow as pa

import zippy

BAR_ENDPOINT = "tcp://127.0.0.1:5556"


def main() -> None:
    subscriber = zippy.ZmqSubscriber(endpoint=BAR_ENDPOINT, timeout_ms=5_000)

    try:
        batch = subscriber.recv()
    finally:
        subscriber.close()

    table = pa.Table.from_batches([batch])
    print(table)


if __name__ == "__main__":
    main()
