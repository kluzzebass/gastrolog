import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { SetupWizard } from "../components/setup/SetupWizard";

export const setupRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/setup",
  component: () => <SetupWizard />,
});
