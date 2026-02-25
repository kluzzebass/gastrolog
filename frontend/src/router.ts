import { createRouter, createRoute, redirect } from "@tanstack/react-router";
import { rootRoute } from "./routes/__root";
import { searchRoute } from "./routes/search";
import { followRoute } from "./routes/follow";
import { loginRoute } from "./routes/login";
import { registerRoute } from "./routes/register";
import { setupRoute } from "./routes/setup";

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  beforeLoad: () => {
    // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router control flow
    throw redirect({ to: "/search", search: { q: "", help: undefined, settings: undefined, inspector: undefined } });
  },
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  searchRoute,
  followRoute,
  loginRoute,
  registerRoute,
  setupRoute,
]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
