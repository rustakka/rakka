import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { ClusterRing } from "@/components/viz/ClusterRing";

describe("ClusterRing", () => {
  it("renders one circle per member plus the ring background", () => {
    const { container } = render(
      <ClusterRing
        members={[
          { address: "akka://a", status: "Up", roles: [], reachable: true, up_number: 1 },
          { address: "akka://b", status: "Up", roles: [], reachable: false, up_number: 2 },
        ]}
      />,
    );
    const circles = container.querySelectorAll("circle");
    expect(circles.length).toBe(1 + 2);
  });
});
