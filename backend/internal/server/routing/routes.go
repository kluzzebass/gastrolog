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
		gastrologv1connect.ConfigServiceGetConfigProcedure:            {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceListIngestersProcedure:        {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceGetIngesterStatusProcedure:    {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceGetSettingsProcedure:          {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceGetPreferencesProcedure:       {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceGetSavedQueriesProcedure:      {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceListCertificatesProcedure:     {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceGetCertificateProcedure:       {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceGetIngesterDefaultsProcedure:  {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceGenerateNameProcedure:         {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceGetRouteStatsProcedure:        {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceListManagedFilesProcedure:     {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceWatchConfigProcedure:          {Strategy: RouteLocal, IsStreaming: true},
		// Node-local operations — run on whichever node received the request.
		gastrologv1connect.ConfigServiceTestIngesterProcedure:         {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceTriggerIngesterProcedure:      {Strategy: RouteLocal, WrapResponse: NewRespWrapper[apiv1.TriggerIngesterResponse]()},
		gastrologv1connect.ConfigServiceTestCloudServiceProcedure:            {Strategy: RouteLocal},
		gastrologv1connect.ConfigServiceTestHTTPLookupProcedure:       {Strategy: RouteLocal},
		gastrologv1connect.ConfigServicePreviewCSVLookupProcedure:     {Strategy: RouteLocal},
		// Config mutations — go through Raft Apply.
		gastrologv1connect.ConfigServicePutFilterProcedure:            {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteFilterProcedure:         {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutRotationPolicyProcedure:    {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteRotationPolicyProcedure: {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutRetentionPolicyProcedure:   {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteRetentionPolicyProcedure: {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutVaultProcedure:              {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteVaultProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutIngesterProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteIngesterProcedure:        {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutSettingsProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceRegenerateJwtSecretProcedure:   {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutPreferencesProcedure:        {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutSavedQueryProcedure:         {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteSavedQueryProcedure:      {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutCertificateProcedure:        {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteCertificateProcedure:     {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePauseVaultProcedure:            {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceResumeVaultProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutNodeConfigProcedure:         {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutRouteProcedure:              {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteRouteProcedure:           {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteManagedFileProcedure:     {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutCloudServiceProcedure:      {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteCloudServiceProcedure:   {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceSetNodeStorageConfigProcedure: {Strategy: RouteLeader},
		gastrologv1connect.ConfigServicePutTierProcedure:              {Strategy: RouteLeader},
		gastrologv1connect.ConfigServiceDeleteTierProcedure:           {Strategy: RouteLeader},

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
	}
}
