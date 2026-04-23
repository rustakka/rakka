import { Route, Routes } from "react-router-dom";
import { ResponsiveShell } from "@/components/layout/ResponsiveShell";
import { useTelemetryStream } from "@/lib/ws";
import { useEventsStore } from "@/store/events";
import Overview from "@/pages/Overview";
import Actors from "@/pages/Actors";
import DeadLetters from "@/pages/DeadLetters";
import Cluster from "@/pages/Cluster";
import Sharding from "@/pages/Sharding";
import Persistence from "@/pages/Persistence";
import Remote from "@/pages/Remote";
import Streams from "@/pages/Streams";
import DData from "@/pages/DData";
import Events from "@/pages/Events";

export function App() {
  const append = useEventsStore((s) => s.append);
  useTelemetryStream({ onEvent: append });

  return (
    <ResponsiveShell>
      <Routes>
        <Route path="/" element={<Overview />} />
        <Route path="/actors" element={<Actors />} />
        <Route path="/dead-letters" element={<DeadLetters />} />
        <Route path="/cluster" element={<Cluster />} />
        <Route path="/sharding" element={<Sharding />} />
        <Route path="/persistence" element={<Persistence />} />
        <Route path="/remote" element={<Remote />} />
        <Route path="/streams" element={<Streams />} />
        <Route path="/ddata" element={<DData />} />
        <Route path="/events" element={<Events />} />
      </Routes>
    </ResponsiveShell>
  );
}
