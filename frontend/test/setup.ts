import { GlobalRegistrator } from "@happy-dom/global-registrator";
GlobalRegistrator.register();

import { afterEach } from "bun:test";
// Dynamic so react-dom loads after the DOM globals exist: it decides at load
// time whether the browser has an `input` event, and without one no text
// input's onChange ever fires.
const { cleanup } = await import("@testing-library/react");

// Unmount rendered components and clear the DOM after each test.
afterEach(cleanup);
