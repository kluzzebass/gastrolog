package routing

import (
	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// DefaultRoutes returns the routing classification for every Connect RPC
// procedure in GastroLog. Every generated *Procedure constant must appear
// here — the coverage test (TestAllProceduresDeclared) enforces this.
//
// Strategy classification rationale:
//   - RouteLocal: reads replicated state or performs node-local computation
//   - RouteLeader: admin mutations that go through Raft Apply
//   - RouteTargeted: must execute on the node that owns the vault
//   - RouteFanOut: handler fans out to all nodes and merges results
func DefaultRoutes() map[string]RPCRoute {
	return map[string]RPCRoute{
		// ── AuthService ──────────────────────────────────────────────────
		// Self-service auth RPCs are RouteLocal — any node can serve them
		// (writes go through Raft Apply transparently).
		gastrologv1connect.AuthServiceRegisterProcedure:       {Strategy: RouteLocal},
		gastrologv1connect.AuthServiceLoginProcedure:          {Strategy: RouteLocal},
		gastrologv1connect.AuthServiceRefreshTokenProcedure:   {Strategy: RouteLocal},
		gastrologv1connect.AuthServiceChangePasswordProcedure: {Strategy: RouteLocal},
		gastrologv1connect.AuthServiceGetAuthStatusProcedure:  {Strategy: RouteLocal},
		gastrologv1connect.AuthServiceListUsersProcedure:      {Strategy: RouteLocal},
		gastrologv1connect.AuthServiceLogoutProcedure:         {Strategy: RouteLocal},
		// Admin user management RPCs are RouteLeader.
		gastrologv1connect.AuthServiceCreateUserProcedure:     {Strategy: RouteLeader},
		gastrologv1connect.AuthServiceUpdateUserRoleProcedure: {Strategy: RouteLeader},
		gastrologv1connect.AuthServiceResetPasswordProcedure:  {Strategy: RouteLeader},
		gastrologv1connect.AuthServiceRenameUserProcedure:     {Strategy: RouteLeader},
		gastrologv1connect.AuthServiceDeleteUserProcedure:     {Strategy: RouteLeader},

		// ── ConfigService ────────────────────────────────────────────────
		// Reads — every node has a full Raft replica.
		gastrologv1connect.SystemServiceGetSystemProcedure:            {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceListIngestersProcedure:        {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceGetIngesterStatusProcedure:    {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceGetSettingsProcedure:          {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceGetPreferencesProcedure:       {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceGetSavedQueriesProcedure:      {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceListCertificatesProcedure:     {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceGetCertificateProcedure:       {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceGetIngesterDefaultsProcedure:  {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceGenerateNameProcedure:         {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceGetRouteStatsProcedure:        {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceListManagedFilesProcedure:     {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceWatchSystemProcedure:          {Strategy: RouteLocal, IsStreaming: true},
		// Node-local operations — run on whichever node received the request.
		gastrologv1connect.SystemServiceTestIngesterProcedure:         {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceTriggerIngesterProcedure:      {Strategy: RouteLocal, WrapResponse: NewRespWrapper[apiv1.TriggerIngesterResponse]()},
		gastrologv1connect.SystemServiceTestCloudServiceProcedure:            {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceTestHTTPLookupProcedure:       {Strategy: RouteLocal},
		gastrologv1connect.SystemServicePreviewCSVLookupProcedure:     {Strategy: RouteLocal},
		gastrologv1connect.SystemServicePreviewJSONLookupProcedure:    {Strategy: RouteLocal},
		gastrologv1connect.SystemServicePreviewYAMLLookupProcedure:    {Strategy: RouteLocal},
		gastrologv1connect.SystemServiceWatchIngesterStatusProcedure:  {Strategy: RouteLocal, IsStreaming: true},
		// Config mutations — go through Raft Apply.
		gastrologv1connect.SystemServicePutFilterProcedure:            {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteFilterProcedure:         {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutRotationPolicyProcedure:    {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteRotationPolicyProcedure: {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutRetentionPolicyProcedure:   {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteRetentionPolicyProcedure: {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutVaultProcedure:              {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteVaultProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutIngesterProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteIngesterProcedure:        {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutServiceSettingsProcedure:   {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutLookupSettingsProcedure:    {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutMaxMindSettingsProcedure:   {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutSetupSettingsProcedure:     {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceRegenerateJwtSecretProcedure:   {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutPreferencesProcedure:        {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutSavedQueryProcedure:         {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteSavedQueryProcedure:      {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutCertificateProcedure:        {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteCertificateProcedure:     {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePauseVaultProcedure:            {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceResumeVaultProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutNodeConfigProcedure:         {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutRouteProcedure:              {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteRouteProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteManagedFileProcedure:     {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutCloudServiceProcedure:      {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteCloudServiceProcedure:   {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceSetNodeStorageConfigProcedure: {Strategy: RouteLeader},
		gastrologv1connect.SystemServicePutTierProcedure:              {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteTierProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.SystemServiceDeleteLookupProcedure:         {Strategy: RouteLeader},

		// ── JobService ───────────────────────────────────────────────────
		gastrologv1connect.JobServiceGetJobProcedure:    {Strategy: RouteLocal},
		gastrologv1connect.JobServiceListJobsProcedure:  {Strategy: RouteLocal},
		gastrologv1connect.JobServiceWatchJobsProcedure: {Strategy: RouteLocal, IsStreaming: true},

		// ── LifecycleService ─────────────────────────────────────────────
		gastrologv1connect.LifecycleServiceHealthProcedure:            {Strategy: RouteLocal},
		gastrologv1connect.LifecycleServiceShutdownProcedure:          {Strategy: RouteLocal},
		gastrologv1connect.LifecycleServiceGetClusterStatusProcedure:  {Strategy: RouteLocal},
		gastrologv1connect.LifecycleServiceJoinClusterProcedure:       {Strategy: RouteLocal},
		gastrologv1connect.LifecycleServiceWatchSystemStatusProcedure: {Strategy: RouteLocal, IsStreaming: true},
		// Cluster mutations — need the Raft leader.
		gastrologv1connect.LifecycleServiceSetNodeSuffrageProcedure: {Strategy: RouteLeader},
		gastrologv1connect.LifecycleServiceRemoveNodeProcedure:      {Strategy: RouteLeader},

		// ── QueryService ─────────────────────────────────────────────────
		// Pure-local reads.
		gastrologv1connect.QueryServiceGetSyntaxProcedure:        {Strategy: RouteLocal},
		gastrologv1connect.QueryServiceValidateQueryProcedure:    {Strategy: RouteLocal},
		gastrologv1connect.QueryServiceGetPipelineFieldsProcedure: {Strategy: RouteLocal},
		// Fan-out — handler queries all nodes and merges results.
		gastrologv1connect.QueryServiceSearchProcedure:       {Strategy: RouteFanOut, IsStreaming: true},
		gastrologv1connect.QueryServiceFollowProcedure:       {Strategy: RouteFanOut, IsStreaming: true},
		gastrologv1connect.QueryServiceExplainProcedure:      {Strategy: RouteFanOut},
		gastrologv1connect.QueryServiceGetContextProcedure:   {Strategy: RouteFanOut},
		gastrologv1connect.QueryServiceGetFieldsProcedure:    {Strategy: RouteFanOut},
		gastrologv1connect.QueryServiceExportToVaultProcedure: {Strategy: RouteFanOut},

		// ── VaultService ─────────────────────────────────────────────────
		// Reads from replicated config / aggregated stats.
		gastrologv1connect.VaultServiceListVaultsProcedure: {Strategy: RouteLocal},
		gastrologv1connect.VaultServiceGetVaultProcedure:   {Strategy: RouteLocal},
		gastrologv1connect.VaultServiceGetStatsProcedure:   {Strategy: RouteLocal},
		// Targeted — must execute on the node that owns the vault.
		// WrapResponse enables the interceptor to deserialize forwarded responses.
		gastrologv1connect.VaultServiceListChunksProcedure:    {Strategy: RouteFanOut},
		gastrologv1connect.VaultServiceGetChunkProcedure:      {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.GetChunkResponse]()},
		gastrologv1connect.VaultServiceGetIndexesProcedure:    {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.GetIndexesResponse]()},
		gastrologv1connect.VaultServiceAnalyzeChunkProcedure:  {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.AnalyzeChunkResponse]()},
		gastrologv1connect.VaultServiceValidateVaultProcedure: {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.ValidateVaultResponse]()},
		gastrologv1connect.VaultServiceSealVaultProcedure:     {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.SealVaultResponse]()},
		gastrologv1connect.VaultServiceReindexVaultProcedure:  {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.ReindexVaultResponse]()},
		gastrologv1connect.VaultServiceExportVaultProcedure:   {Strategy: RouteTargeted, IsStreaming: true}, // streaming — handler manages routing
		gastrologv1connect.VaultServiceImportRecordsProcedure: {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.ImportRecordsResponse]()},
		gastrologv1connect.VaultServiceMigrateVaultProcedure:  {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.MigrateVaultResponse]()},
		gastrologv1connect.VaultServiceMergeVaultsProcedure:   {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.MergeVaultsResponse]()},
		gastrologv1connect.VaultServiceArchiveChunkProcedure:  {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.ArchiveChunkResponse]()},
		gastrologv1connect.VaultServiceRestoreChunkProcedure: {Strategy: RouteTargeted, WrapResponse: NewRespWrapper[apiv1.RestoreChunkResponse]()},
		gastrologv1connect.VaultServiceWatchChunksProcedure:  {Strategy: RouteLocal, IsStreaming: true},
	}
}
