"""Browser regression checks for the sample-data demo; no backend credentials needed."""
import json
import os
from playwright.sync_api import sync_playwright

base = os.environ.get('DEMO_BASE_URL', 'http://127.0.0.1:3004').rstrip('/')
with sync_playwright() as p:
    browser = p.chromium.launch(executable_path=os.environ.get('PLAYWRIGHT_CHROMIUM_EXECUTABLE'), headless=True, args=['--no-sandbox'])
    context = browser.new_context(viewport={'width': 1440, 'height': 1000}, permissions=['clipboard-read','clipboard-write'])
    context.add_init_script("if(!sessionStorage.getItem('migration-seeded')){localStorage.setItem('kafkaui-demo-v1','legacy demo state');sessionStorage.setItem('migration-seeded','true')}")
    page = context.new_page()
    errors=[]; external=[]
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.on('request', lambda req: external.append(req.url) if '/api/' in req.url else None)
    page.goto(base+'/clusters/demo/topics/demo.orders')
    page.get_by_role('button',name='Messages',exact=True).click()
    page.get_by_text('Showing 24 messages',exact=True).wait_for()
    page.get_by_role('button',name='Inspect message partition 0 offset 0',exact=True).click()
    def copy(label):
        page.get_by_role('button',name=label,exact=True).click()
        return page.evaluate('navigator.clipboard.readText()')
    assert page.evaluate("localStorage.getItem('kafkaui-demo-v1')") is None
    whole=json.loads(copy('Copy whole message'))
    assert whole['metadata']['topic']=='demo.orders' and whole['metadata']['partition']==0
    assert copy('Copy message key')==whole['key']
    assert copy('Copy message value')==whole['value']
    assert json.loads(copy('Copy message metadata'))==whole['metadata']
    assert json.loads(copy('Copy all message headers'))==whole['headers']
    assert copy('Copy header source')=='sample-data'
    page.evaluate("Object.defineProperty(navigator.clipboard,'writeText',{value:async()=>{throw new Error('denied')},configurable:true})")
    page.get_by_role('button',name='Copy message key',exact=True).click()
    page.get_by_text('Clipboard unavailable. Select and copy the text manually.',exact=True).wait_for()
    page.get_by_role('button',name='Consumers',exact=True).click()
    page.get_by_role('link',name='analytics-orders',exact=True).click()
    page.wait_for_url('**/consumer-groups/analytics-orders')
    page.get_by_role('link',name='demo.payments',exact=True).wait_for()
    orders=page.get_by_role('button',name='Expand partitions for demo.orders',exact=True)
    assert orders.get_attribute('aria-expanded')=='false'
    assert page.get_by_role('columnheader',name='Partition',exact=True).count()==0
    orders.click()
    assert page.get_by_role('columnheader',name='Partition',exact=True).count()==1
    page.get_by_role('button',name='Expand all',exact=True).click()
    assert page.get_by_role('columnheader',name='Partition',exact=True).count()==2
    page.get_by_role('button',name='Collapse all',exact=True).click()
    assert page.get_by_role('columnheader',name='Partition',exact=True).count()==0
    page.get_by_role('link',name='demo.payments',exact=True).click()
    page.wait_for_url('**/topics/demo.payments')
    page.get_by_role('button',name='Consumers',exact=True).click()
    page.get_by_role('link',name='analytics-orders',exact=True).wait_for()
    assert page.get_by_role('link',name='notifications-service',exact=True).count()==0
    page.goto(base+'/jobs')
    page.get_by_role('button',name='New Job',exact=False).click()
    page.locator('select').nth(0).select_option('demo')
    page.locator('select').nth(1).select_option('demo.orders')
    page.get_by_placeholder('e.g. trip-analytics-service').fill('preview-copy-test')
    page.get_by_placeholder('e.g. data-engineering').fill('data-platform')
    page.get_by_role('button',name='Next →',exact=True).click()
    page.get_by_role('button',name='Next →',exact=True).click()
    page.get_by_role('combobox',name='Managed connection').select_option('demo-vault')
    assert page.locator('input[type=password]').count()==0
    page.get_by_role('button',name='Next →',exact=True).click()
    page.get_by_role('button',name='Create Job',exact=True).click()
    page.get_by_text('preview-copy-test → demo.orders',exact=True).first.wait_for()
    stored=json.loads(page.evaluate("localStorage.getItem('kafkaui-demo-v2')"))
    assert stored['jobs'][-1]['destinationConfig']['connectionId']=='demo-vault'
    assert not stored['jobs'][-1]['destinationConfig'].get('password')
    assert not errors,errors
    assert not external,external
    print('PASS: clipboard payloads/errors, consumer navigation, collapsed multi-topic lag, managed connection job creation; no API requests or browser errors')
    browser.close()
