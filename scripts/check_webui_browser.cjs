/* Exercise the production console bundle with controlled HTTP races.
 * Usage: node scripts/check_webui_browser.cjs /path/to/playwright-core /path/to/chrome /tmp/output
 */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { chromium } = require(path.resolve(process.argv[2]));
const chrome = process.argv[3];
const output = process.argv[4];
const root = path.resolve(__dirname, '../python/zippy/webui_static');
const tables = ['alpha', 'beta'].map(namespace => ({
  namespace, name: 'ticks', catalog: 'zippy', table_type: 'TABLE',
}));
const schema = { fields: [
  { name: 'id', data_type: 'int64', nullable: false },
  { name: 'ns', data_type: 'utf8', nullable: false },
  { name: 'large', data_type: 'int64', nullable: true },
  { name: 'dt', data_type: 'timestamp[ns, tz=UTC]', nullable: true },
] };
const snapshots = [{ table_id: 'native-fixed-table', snapshot_version: 7 }];
const requests = [];
const errors = [];
const server = http.createServer((req, res) => {
  const relative = req.url === '/' ? 'index.html' : decodeURIComponent(req.url.slice(1));
  if (relative === 'assets/zippy-logo.png') {
    res.setHeader('Content-Type', 'image/png');
    res.end(fs.readFileSync(path.resolve(root, '../assets/zippy-logo.png'))); return;
  }
  const target = path.resolve(root, relative);
  if (!target.startsWith(root + path.sep) || !fs.existsSync(target)) {
    res.writeHead(404).end(); return;
  }
  res.setHeader('Content-Type', target.endsWith('.js') ? 'text/javascript' : target.endsWith('.css') ? 'text/css' : 'text/html');
  res.end(fs.readFileSync(target));
});

(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const browser = await chromium.launch({ executablePath: chrome, headless: true, args: ['--no-sandbox'] });
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });
    page.on('pageerror', error => errors.push(String(error)));
    await page.route('**/api/dashboard', route => route.fulfill({ json: {
      generated_at: new Date().toISOString(), webui: { pid: 1, uptime_sec: 1, host: '127.0.0.1', port: 0 },
      catalog: { status: 'ok', error: null }, pm: { status: 'ok', error: null },
      totals: { tables: 2, namespaces: 2, tasks: 0 }, tables, tasks: [], log_tail: [],
    } }));
    let slowAlpha = false;
    await page.route('**/api/table/**', async route => {
      const url = new URL(route.request().url());
      const namespace = url.searchParams.get('namespace');
      requests.push({ namespace, mode: url.searchParams.get('mode'), order: url.searchParams.getAll('order_by') });
      assert(['alpha', 'beta'].includes(namespace));
      if (slowAlpha && namespace === 'alpha') await new Promise(resolve => setTimeout(resolve, 600));
      const table = tables.find(item => item.namespace === namespace);
      const json = url.pathname.endsWith('/data') ? {
        ok: true, table_name: 'ticks', namespace, mode: url.searchParams.get('mode'),
        limit: Number(url.searchParams.get('limit')), schema, snapshots,
        columns: schema.fields.map(field => field.name), row_count: 1, truncated: false,
        rows: [{ id: 2, ns: namespace, large: '9223372036854775807', dt: '1970-01-01 00:00:00.000000001Z' }],
      } : { ok: true, table: { ...table, schema, snapshots } };
      await route.fulfill({ json }).catch(error => {
        if (!route.request().failure()) throw error;
      });
    });
    await page.goto(`http://127.0.0.1:${server.address().port}/`);
    await page.getByRole('cell', { name: 'alpha', exact: true }).click();
    await page.getByText('native-fixed-table', { exact: true }).waitFor();
    await page.getByRole('button', { name: 'Data', exact: true }).click();
    await page.getByText('9223372036854775807', { exact: true }).waitFor();
    assert(await page.getByRole('button', { name: 'Load tail rows' }).isDisabled());
    assert.equal(await page.locator('.data-column-type').last().textContent(), 'timestamp[ns, tz=UTC]');
    await page.getByLabel('Sort columns', { exact: true }).click();
    await page.getByRole('checkbox', { name: 'id', exact: true }).check();
    await page.getByRole('checkbox', { name: 'ns', exact: true }).check();
    await page.getByLabel('Sort columns', { exact: true }).click();
    await page.getByRole('button', { name: 'Load tail rows' }).click();
    await page.waitForFunction(() => document.querySelector('.data-table tbody')?.textContent.includes('9223372036854775807'));
    assert(requests.some(item => item.mode === 'tail' && item.order.join(',') === 'id,ns'));
    slowAlpha = true;
    await page.getByRole('button', { name: 'Refresh data', exact: true }).click();
    await page.getByRole('button', { name: 'beta / ticks', exact: true }).click();
    await page.waitForFunction(() => document.querySelector('.data-table tbody')?.textContent.includes('beta'));
    await new Promise(resolve => setTimeout(resolve, 800));
    assert((await page.locator('.data-table tbody').textContent()).includes('beta'));
    assert(!(await page.locator('.data-table tbody').textContent()).includes('alpha'));
    assert.equal(await page.locator('.inspector-name').textContent(), 'beta / ticks');
    assert(await page.getByRole('button', { name: 'Load tail rows' }).isDisabled());
    await page.getByLabel('Export data', { exact: true }).click();
    const pendingDownload = page.waitForEvent('download');
    await page.getByRole('button', { name: 'Export JSON', exact: true }).click();
    const download = await pendingDownload;
    const downloaded = JSON.parse(fs.readFileSync(await download.path(), 'utf8'));
    assert.equal(downloaded[0].large, '9223372036854775807');
    assert.equal(downloaded[0].ns, 'beta');
    assert.equal(download.suggestedFilename(), 'beta-ticks.json');
    await page.getByLabel('Export data', { exact: true }).click();
    await page.screenshot({ path: `${output}-wide.png`, fullPage: true });
    await page.setViewportSize({ width: 900, height: 900 });
    const toolbarFits = await page.locator('.data-toolbar').evaluate(element => {
      const bounds = element.getBoundingClientRect();
      return [...element.querySelectorAll('button')].every(button => {
        const rect = button.getBoundingClientRect();
        return rect.width === 0 || (rect.left >= bounds.left && rect.right <= bounds.right + 1);
      });
    });
    assert(toolbarFits, 'toolbar controls overflow at narrow width');
    await page.screenshot({ path: `${output}-narrow.png`, fullPage: true });
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ ok: true, checks: ['namespace_identity', 'explicit_sort', 'tail_requires_order', 'request_cancellation', 'snapshot_schema', 'exact_integer_render_and_export'], requests: requests.length }));
  } finally {
    await browser.close();
    await new Promise(resolve => server.close(resolve));
  }
})().catch(error => { console.error(error); server.close(); process.exitCode = 1; });
