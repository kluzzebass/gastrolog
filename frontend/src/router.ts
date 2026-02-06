import { createRouter, createRoute, redirect } from "@tanstack/react-router";
import { rootRoute } from "./routes/__root";
import { searchRoute } from "./routes/search";
import { followRoute } from "./routes/follow";

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  beforeLoad: () => {
    throw redirect({ to: "/search" });
  },
});

const routeTree = rootRoute.addChildren([indexRoute, searchRoute, followRoute]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
