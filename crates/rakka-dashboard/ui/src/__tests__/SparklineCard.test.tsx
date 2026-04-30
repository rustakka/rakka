import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { SparklineCard } from "@/components/viz/SparklineCard";

describe("SparklineCard", () => {
  it("renders the title and value", () => {
    render(
      <SparklineCard title="Actors" value={1234} history={[1, 2, 3, 4]} />,
    );
    expect(screen.getByText(/Actors/i)).toBeInTheDocument();
    expect(screen.getByText(/1\.2k/i)).toBeInTheDocument();
  });
});
