import { relative } from 'node:path';

// Vitest 3.2 reporter API. No errors, payloads or task objects are serialized.
export default class CiTestReporter {
  mark(event, module, name, state) {
    console.log(`[ci-test] ${JSON.stringify({
      time: new Date().toISOString(), event,
      file: relative(process.cwd(), module.moduleId), name, state,
    })}`);
  }
  onTestModuleStart(module) { this.mark('module-start', module); }
  onTestModuleEnd(module) { this.mark('module-end', module, undefined, module.state()); }
  onTestCaseReady(test) { this.mark('test-start', test.module, test.fullName); }
  onTestCaseResult(test) { this.mark('test-end', test.module, test.fullName, test.result().state); }
}
