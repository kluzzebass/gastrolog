import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { EditorialDesign } from "../designs/editorial/EditorialDesign";

export const followRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/follow",
  validateSearch: (search: Record<string, unknown>) => ({
    q: (search.q as string) || "",
  }),
  component: EditorialDesign,
});
