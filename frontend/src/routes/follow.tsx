import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { SearchView } from "../components/SearchView";

export const followRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/follow",
  validateSearch: (search: Record<string, unknown>) => ({
    q: (search.q as string) || "",
    help: (search.help as string) || undefined,
    settings: (search.settings as string) || undefined,
    inspector: (search.inspector as string) || undefined,
  }),
  component: SearchView,
});
