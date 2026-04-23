// Tiny helper to maintain a bounded history series for sparklines.

import { useEffect, useRef, useState } from "react";

export function useHistory(value: number, cap = 60): number[] {
  const [hist, setHist] = useState<number[]>(() => [value]);
  const lastRef = useRef<number | null>(null);

  useEffect(() => {
    if (lastRef.current === value) return;
    lastRef.current = value;
    setHist((prev) => {
      const next = prev.concat(value);
      if (next.length > cap) next.splice(0, next.length - cap);
      return next;
    });
  }, [value, cap]);

  return hist;
}
