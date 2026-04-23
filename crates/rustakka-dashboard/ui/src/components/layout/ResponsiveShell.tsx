import type { ReactNode } from "react";
import { Sidebar } from "./Sidebar";
import { BottomTabs } from "./BottomTabs";
import { TopBar } from "./TopBar";

export function ResponsiveShell({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-svh w-full">
      <Sidebar />
      <div className="flex min-w-0 flex-1 flex-col">
        <TopBar />
        <main className="flex-1 overflow-auto p-3 pb-20 md:p-6 md:pb-6">{children}</main>
      </div>
      <BottomTabs />
    </div>
  );
}
