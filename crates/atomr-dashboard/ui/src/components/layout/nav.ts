import {
  Activity,
  Boxes,
  Database,
  GitBranch,
  Inbox,
  LayoutDashboard,
  MoreHorizontal,
  Network,
  Orbit,
  Share2,
  Waves,
} from "lucide-react";

export interface NavItem {
  label: string;
  to: string;
  icon: typeof LayoutDashboard;
  mobilePrimary?: boolean;
}

export const NAV_ITEMS: NavItem[] = [
  { label: "Overview", to: "/", icon: LayoutDashboard, mobilePrimary: true },
  { label: "Actors", to: "/actors", icon: Boxes, mobilePrimary: true },
  { label: "Topology", to: "/topology", icon: Orbit, mobilePrimary: true },
  { label: "Cluster", to: "/cluster", icon: Share2, mobilePrimary: true },
  { label: "Dead letters", to: "/dead-letters", icon: Inbox },
  { label: "Persistence", to: "/persistence", icon: Database, mobilePrimary: true },
  { label: "Sharding", to: "/sharding", icon: GitBranch },
  { label: "Remote", to: "/remote", icon: Network },
  { label: "Streams", to: "/streams", icon: Waves },
  { label: "DData", to: "/ddata", icon: Activity },
  { label: "Events", to: "/events", icon: MoreHorizontal, mobilePrimary: true },
];
