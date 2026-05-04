import { NavLink } from "react-router-dom";
import { cn } from "@/lib/utils";
import { NAV_ITEMS } from "./nav";

export function BottomTabs() {
  const items = NAV_ITEMS.filter((i) => i.mobilePrimary);
  return (
    <nav className="md:hidden fixed inset-x-0 bottom-0 z-40 flex border-t bg-card/95 backdrop-blur px-2 py-1 pb-[max(env(safe-area-inset-bottom),0.25rem)]">
      {items.map((item) => (
        <NavLink
          key={item.to}
          to={item.to}
          end={item.to === "/"}
          className={({ isActive }) =>
            cn(
              "flex flex-1 flex-col items-center gap-0.5 rounded-md py-1 text-[10px] font-medium text-muted-foreground",
              isActive && "text-foreground",
            )
          }
        >
          <item.icon className="size-5" aria-hidden />
          <span>{item.label}</span>
        </NavLink>
      ))}
    </nav>
  );
}
