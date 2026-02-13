import { GlobalRegistrator } from "@happy-dom/global-registrator";
GlobalRegistrator.register();

import { afterEach } from "bun:test";
import { cleanup } from "@testing-library/react";

// Unmount rendered components and clear the DOM after each test.
afterEach(cleanup);
