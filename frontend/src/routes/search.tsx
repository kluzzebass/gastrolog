import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { EditorialDesign } from "../designs/editorial/EditorialDesign";

export const searchRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  validateSearch: (search: Record<string, unknown>) => ({
    q: (search.q as string) || "",
  }),
  component: EditorialDesign,
});
