import { lazy } from "react";
import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";

const SetupWizard = lazy(() => import("../components/setup/SetupWizard").then((m) => ({ default: m.SetupWizard })));

export const setupRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/setup",
  component: () => <SetupWizard />,
});
