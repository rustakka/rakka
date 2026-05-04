import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { ResponsiveShell } from "@/components/layout/ResponsiveShell";

describe("ResponsiveShell", () => {
  it("renders children, nav, and a top bar", () => {
    render(
      <MemoryRouter initialEntries={["/"]}>
        <ResponsiveShell>
          <div data-testid="content">hello</div>
        </ResponsiveShell>
      </MemoryRouter>,
    );
    expect(screen.getByTestId("content")).toBeInTheDocument();
    expect(screen.getAllByText(/Overview/i).length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText(/Actors/i).length).toBeGreaterThanOrEqual(1);
  });
});
