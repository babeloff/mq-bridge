import type { PeerStatus } from "../runtime-status";
import { EMPTY_PEER_STATUS } from "../runtime-status";

let peerStatus = $state<PeerStatus>(EMPTY_PEER_STATUS);

export function getPeerStatus(): PeerStatus {
  return peerStatus;
}

export function setPeerStatus(status: PeerStatus): void {
  peerStatus = status;
}
