import { test, expect } from "@playwright/test";

const ROUTES: { path: string; heading: RegExp }[] = [
  { path: "/", heading: /Overview/i },
  { path: "/actors", heading: /Actor hierarchy/i },
  { path: "/dead-letters", heading: /Dead letters/i },
  { path: "/cluster", heading: /Topology|Members/i },
  { path: "/sharding", heading: /Regions|allocations/i },
  { path: "/persistence", heading: /Total events|Recent writes/i },
  { path: "/remote", heading: /Remote associations/i },
  { path: "/streams", heading: /Running|Active graphs/i },
  { path: "/ddata", heading: /Total updates|Keys/i },
  { path: "/events", heading: /Events/i },
];

for (const r of ROUTES) {
  test(`${r.path} renders`, async ({ page }) => {
    await page.goto(r.path);
    await expect(page.getByText(r.heading).first()).toBeVisible();
  });
}
