import { useEffect, useState } from "react";
import { Moon, Sun, RadioTower } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { useConnectionStore } from "@/store/connection";

export function TopBar() {
  const { node, mode } = useConnectionStore();
  const [dark, setDark] = useState(() =>
    document.documentElement.classList.contains("dark"),
  );

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
  }, [dark]);

  return (
    <header className="sticky top-0 z-30 flex h-12 items-center gap-3 border-b bg-background/85 px-3 backdrop-blur">
      <div className="flex md:hidden items-center gap-1 text-sm font-semibold">
        <span className="text-primary">rust</span>akka
      </div>
      <div className="ml-auto flex items-center gap-2">
        <Badge variant="outline" className="gap-1">
          <RadioTower className="size-3" aria-hidden />
          {mode}
        </Badge>
        <Badge variant="default">{node}</Badge>
        <button
          type="button"
          aria-label="toggle theme"
          className="rounded-md border p-1 text-muted-foreground hover:text-foreground"
          onClick={() => setDark((d) => !d)}
        >
          {dark ? <Sun className="size-4" /> : <Moon className="size-4" />}
        </button>
      </div>
    </header>
  );
}
