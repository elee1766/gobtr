import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { FilesystemService } from "%/v1/filesystem_pb";
import { SubvolumeService } from "%/v1/subvolume_pb";
import { ScrubService } from "%/v1/scrub_pb";
import { BalanceService } from "%/v1/balance_pb";
import { HealthService } from "%/v1/health_pb";
import { UsageService } from "%/v1/usage_pb";
import { FragMapService } from "%/v1/fragmap_pb";

const transport = createConnectTransport({
  baseUrl: window.location.origin,
});

export const filesystemClient = createClient(FilesystemService, transport);
export const subvolumeClient = createClient(SubvolumeService, transport);
export const scrubClient = createClient(ScrubService, transport);
export const balanceClient = createClient(BalanceService, transport);
export const healthClient = createClient(HealthService, transport);
export const usageClient = createClient(UsageService, transport);
export const fragmapClient = createClient(FragMapService, transport);
