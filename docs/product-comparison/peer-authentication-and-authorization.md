# Peer authentication, authorization & multi-tenancy (product parity research)

Part of **product comparison** docs: see [overview](./overview.md) for the full peer list and when to use each system as a reference.

This doc summarizes what common **peer** log/search/observability products implement for:

- **Authentication (AuthN)**: how users and API clients prove identity.
- **Authorization (AuthZ)**: how access is granted and limited (RBAC, privileges, data-level filters).
- **Multi-tenancy**: how deployments separate customers, teams, or environments (SaaS vs self-managed where it matters).

The goal is **feature parity**: to spell out what Gastrolog itself may need to implement so operators and end users get capabilities they already expect from these products—not how to integrate Gastrolog *with* Splunk, Graylog, or the others.

## Index of compared products

| Product | What this doc covers |
|--------|----------------------|
| **Splunk** | Splunk Enterprise and Splunk Cloud Platform (core platform auth, roles/capabilities, index and knowledge-object scoping; SOAR multi-tenant note only as contrast). |
| **Graylog** | Graylog Open, Enterprise, and Cloud (LDAP/OIDC/SAML, roles + sharing, streams/index sets). |
| **Elastic** | Elasticsearch + Kibana (Stack): auth providers, roles, Spaces, index privileges, DLS/FLS. |
| **OpenSearch** | OpenSearch + OpenSearch Dashboards with Security plugin (auth backends, roles, tenants). |
| **Grafana** | Grafana as the control plane often used with **Loki** (orgs, folders, SSO/RBAC)—reference for expected UI/tenancy patterns, not Loki internals. |
| **Datadog** | Datadog Logs and org/account model (SAML, RBAC, restriction queries, multi-org). |
| **Sumo Logic** | SAML, RBAC (capabilities + role search filters), organizations / multi-account behavior. |

Sections below follow this order, then a **parity checklist** for Gastrolog.

## Splunk (Splunk Enterprise / Splunk Cloud Platform)

### Authentication (AuthN)
- **Native (built-in) auth** (“Native Splunk authentication”), which “takes precedence over any external authentication schemes.”  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Security/SetupuserauthenticationwithSplunk`
- **LDAP** (authenticate against Splunk internal auth services or an external LDAP server).  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Security/SetupuserauthenticationwithSplunk`
- **SAML 2.0** (SSO via an IdP; SAML attributes can be mapped to Splunk roles).  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Security/SetupuserauthenticationwithSplunk`
- **MFA** (Splunk Enterprise; includes Duo or RSA Manager).  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Security/SetupuserauthenticationwithSplunk`
- **Scripted auth API** (Splunk Enterprise; integrate with external systems such as RADIUS or PAM).  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Security/SetupuserauthenticationwithSplunk`

### Multi-tenancy
- **Splunk Enterprise** (self-managed) is commonly “multi-tenant” by convention (separate indexes/apps/roles) rather than a first-class “tenant object” in the core platform. In practice, tenancy boundaries are typically modeled with **index restrictions**, **role search filters**, and **app / knowledge-object sharing scope** (see below).
- **Splunk Cloud Platform** tenancy is typically realized as separate cloud deployments / org arrangements rather than a single shared control plane with explicit tenants (not covered in the core platform docs cited here).
- **Splunk SOAR** (separate product) explicitly supports multiple tenants on a single instance, but that is not the Splunk platform’s core tenancy model.  
  Source: `https://help.splunk.com/?resourceId=SOARonprem_Admin_MultiTenancy`

### Authorization (AuthZ)
- **RBAC via roles + capabilities**: users are assigned one or more roles; roles contain capabilities that define what users can do.  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Security/Rolesandcapabilities`
- **Additive permissions**: capabilities are additive; you can’t remove access by adding more capabilities.  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Security/Rolesandcapabilities`

### AuthZ granularity (what you can scope)
- **Index-level search boundaries per role** via `authorize.conf`:
  - `srchIndexesAllowed`: “A list of indexes that this role is allowed to search.”
  - `srchIndexesDisallowed`: “takes precedence” over allowed/default lists.
  - `srchIndexesDefault`: indexes searched when no index is specified.  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Admin/Authorizeconf`
- **Role search filters** via `srchFilter` (and inheritance behavior via `srchFilterSelecting`), which combine filters when roles inherit from other roles (OR/AND semantics depending on selecting vs eliminating).  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Admin/Authorizeconf`
- **Knowledge object sharing scope + per-role permissions**:
  - Knowledge objects start private (owner-only) and can be made available to **all apps (global)**, **an app**, and can be restricted/expanded by **role**.
  - Splunk calls out setting **read/write permissions at the app level for roles** to allow sharing/deleting objects users do not own.  
  Source: `https://docs.splunk.com/Documentation/Splunk/latest/Knowledge/Manageknowledgeobjectpermissions`

## Graylog (Graylog Open / Graylog Enterprise / Graylog Cloud)

### Authentication (AuthN)
- **Local/manual users** are supported; Graylog recommends keeping a local admin account even when SSO is enabled.  
  Source: `https://go2docs.graylog.org/current/setting_up_graylog/user_authentication.htm`
- **SSO / IdP integrations** (web interface):
  - Graylog Open: **Active Directory / LDAP**
  - Graylog Enterprise: adds **Okta**, **OpenID Connect (OIDC)**, and **SAML**
  Source: `https://go2docs.graylog.org/current/setting_up_graylog/user_authentication.htm`
- **Trusted header authentication** (proxy performs auth and passes a configured HTTP header; used to front Graylog with external auth systems like Kerberos).  
  Source: `https://go2docs.graylog.org/current/setting_up_graylog/user_authentication.htm`

### Authorization (AuthZ)
- **Two-part model: roles + sharing**. A user needs (1) an appropriate role and (2) the entity shared with them to access it.  
  Source: `https://go2docs.graylog.org/current/setting_up_graylog/permission_management.html`
- **Roles define capabilities** (e.g., Reader/Viewer, Manager, Creator; plus combined `Admin` and `Reader`). Graylog notes every user must have either `Reader` or `Admin`.  
  Source: `https://go2docs.graylog.org/current/setting_up_graylog/permission_management.html`
- **Sharing levels** for entities: Viewer / Manager / Owner (for streams, dashboards, saved searches, event definitions, alerts, etc.).  
  Source: `https://go2docs.graylog.org/current/setting_up_graylog/permission_management.html`

### Multi-tenancy
- Graylog’s primary “tenancy” building blocks are usually **streams** (filtered subsets of data) + **index sets** (storage destinations/retention/mapping policy) combined with the **roles + sharing** permission model.
- Streams are explicitly described as “a filtered subset of your log data” and can route to specific index sets and destinations; the Streams UI also supports “Share” to grant stream access to specific users/teams.  
  Source: `https://go2docs.graylog.org/current/making_sense_of_your_log_data/streams.html`

### AuthZ granularity (what you can scope)
- **Entity-level access** is gated by both **role** and **sharing** (two-part model).  
  Source: `https://go2docs.graylog.org/current/setting_up_graylog/permission_management.html`
- **Stream-level control** (practical data partition): since streams represent filtered subsets and are shareable, they are a common unit for tenant-ish isolation (e.g., “Team A stream”, “Customer B stream”).  
  Source: `https://go2docs.graylog.org/current/making_sense_of_your_log_data/streams.html`

## Elastic Stack (Elasticsearch + Kibana)

### Authentication (AuthN)
Kibana supports multiple auth providers simultaneously (configured as a prioritized list).  
Source: `https://www.elastic.co/guide/en/kibana/8.18/kibana-authentication.html`

Examples called out in Elastic’s Kibana guide:
- **Basic authentication** (username/password; based on Elasticsearch realms such as Native / LDAP / Active Directory).  
  Source: `https://www.elastic.co/guide/en/kibana/8.18/kibana-authentication.html`
- **Token authentication** (uses Elasticsearch token APIs; subscription feature).  
  Source: `https://www.elastic.co/guide/en/kibana/8.18/kibana-authentication.html`
- **PKI authentication** (X.509 client certs; subscription feature).  
  Source: `https://www.elastic.co/guide/en/kibana/8.18/kibana-authentication.html`
- **SAML SSO** (subscription feature).  
  Source: `https://www.elastic.co/guide/en/kibana/8.18/kibana-authentication.html`

### Authorization (AuthZ)
- **RBAC via roles**: roles are collections of privileges in Kibana/Elasticsearch; users are assigned roles (not privileges directly).  
  Source: `https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/kibana-role-management`
- **Union of privileges**: multiple roles yield a union; to restrict you must remove/edit roles.  
  Source: `https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/kibana-role-management`
- **Index privileges** per role; Elastic recommends `read` + `view_index_metadata` for indices used in Kibana.  
  Source: `https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/kibana-role-management`
- **Document-level (DLS) and field-level security (FLS)** for finer-grained data access (noted as subscription features).  
  Source: `https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/kibana-role-management`
- **Kibana privileges and Spaces** allow scoping feature access to specific spaces.  
  Source: `https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/kibana-role-management`

### Multi-tenancy
- **Kibana Spaces** are a first-class tenancy/workspace boundary: each space has its own saved objects; users can access only spaces granted by role; and a role can have different permissions per space.  
  Source: `https://www.elastic.co/docs/deploy-manage/manage-spaces`

### AuthZ granularity (what you can scope)
- **Space-scoped feature access**: per-space permissions can vary for the same role, but Elastic notes that “controlling feature visibility is not a security feature” and per-user security must be configured via roles.  
  Source: `https://www.elastic.co/docs/deploy-manage/manage-spaces`
- **Document-level security (DLS)**: restrict documents per role using a query associated with an index/data stream pattern; the query can use templating against the current authenticated user; omitting `query` disables DLS for that entry.  
  Source: `https://elastic.co/guide/en/elasticsearch/reference/current/document-level-security.html`
- **Field-level security (FLS)**: restrict which fields a role can access per index/data stream pattern; omitting field lists disables FLS.  
  Source: `https://elastic.co/guide/en/elasticsearch/reference/current/document-level-security.html`
- **Multiple roles caveat (DLS/FLS)**: DLS queries are combined with OR across roles; FLS fields are unioned; if any role grants access without DLS/FLS, those restrictions may not apply. Elastic explicitly warns about inadvertently granting wider access.  
  Source: `https://elastic.co/guide/en/elasticsearch/reference/current/document-level-security.html`

## OpenSearch (OpenSearch + OpenSearch Dashboards; Security plugin)

### Authentication (AuthN)
- **Dashboards sign-in options**: supports **basic authentication**, **OpenID Connect**, and **SAML** (and can expose multiple options on the login screen).  
  Source: `https://opensearch.org/docs/latest/security/configuration/multi-auth/`

### Authorization (AuthZ)
- **RBAC roles** can include cluster permissions, index-specific permissions, document/field-level security, and tenant access.  
  Source: `https://opensearch.org/docs/latest/security/access-control/users-roles/`
- **User/role mapping** determines effective permissions; users can be internal or external (for example LDAP/AD).  
  Source: `https://opensearch.org/docs/latest/security/access-control/users-roles/`
- **Tenants / multi-tenancy** are part of the model (roles can grant tenant permissions; Dashboards can also enforce a UI-level read-only mode via `readonly_mode`).  
  Source: `https://opensearch.org/docs/latest/security/access-control/users-roles/`
- **Super admin via certificates** (authenticated through certificates, not passwords).  
  Source: `https://opensearch.org/docs/latest/security/access-control/users-roles/`

### Multi-tenancy
- **Dashboards multi-tenancy** is enabled by default; tenants include global and private tenants, plus custom tenants; tenant selection is carried via the `securitytenant` header between Dashboards and OpenSearch.  
  Source: `https://opensearch.org/docs/latest/security/multi-tenancy/multi-tenancy-config/`
- The Security plugin uses a single `.kibana` index for the global tenant and separate per-tenant/per-user indexes for other tenants (e.g. `.kibana_<hash>_<tenant_name>`).  
  Source: `https://opensearch.org/docs/latest/security/multi-tenancy/multi-tenancy-config/`

### AuthZ granularity (what you can scope)
- **Tenant permissions**: roles can be given read-only (`kibana_all_read`) or read-write (`kibana_all_write`) access per tenant.  
  Source: `https://opensearch.org/docs/latest/security/multi-tenancy/multi-tenancy-config/`

## Grafana (commonly paired with Loki for logs)

Grafana is included as a peer reference for **what a full log exploration stack often exposes** (orgs, folders, SSO): many teams use Loki with Grafana in front, so this is a useful bar for Gastrolog’s **own** UI and tenancy model—not an instruction to embed or integrate Grafana.

### Authentication (AuthN)
- Grafana supports many auth integrations (examples include **SAML** (Enterprise/Cloud), **LDAP**, **OAuth**, **auth proxy**, **JWT proxy**, **basic auth**, and **anonymous** access).  
  Source: `https://grafana.com/docs/grafana/latest/setup-grafana/configure-security/configure-authentication`
- Grafana notes it **does not include built-in MFA**, recommending an external IdP that supports MFA instead.  
  Source: `https://grafana.com/docs/grafana/latest/setup-grafana/configure-security/configure-authentication`

### Authorization (AuthZ)
- **RBAC (Enterprise/Cloud)**: roles consist of permissions with an **action** and **scope**; RBAC extends the basic roles available in OSS.  
  Source: `https://grafana.com/docs/grafana/latest/administration/roles-and-permissions/access-control`

### Multi-tenancy
- **Organizations** are Grafana’s first-class multi-tenancy boundary: an organization “helps you isolate users and resources such as dashboards, annotations, and data sources” and provides “completely separate experiences…within a single instance.”  
  Source: `https://grafana.com/docs/grafana/v10.4/administration/organization-management/`
- Grafana documents which resources are isolated vs shared across organizations (notably: dashboards/data sources/alerts/teams are isolated; auth providers/config are shared).  
  Source: `https://grafana.com/docs/grafana/v10.4/administration/organization-management/`

### AuthZ granularity (what you can scope)
- **Folder access control**: folder permissions are a primary unit for controlling access and cascade to dashboards, alert rules, SLOs, etc.; permission levels are View/Edit/Admin with explicit action breakdown.  
  Source: `https://grafana.com/docs/grafana/latest/administration/roles-and-permissions/folder-access-control/`

## Datadog (Logs)

### Authentication (AuthN)
- **SAML SSO** is supported; configuration requires admin / org management permissions, and enterprise customers can configure multiple SAML IdPs per org (up to three).  
  Source: `https://docs.datadoghq.com/account_management/saml/configuration/`

### Authorization (AuthZ)
- **RBAC permissions for Logs** exist for both configuration operations and data access (for example permissions around indexes, pipelines, archives, and reading log data).  
  Source: `https://docs.datadoghq.com/logs/guide/logs-rbac-permissions`
- **Data-level restrictions** can be applied using restriction queries; Datadog notes roles are additive and effective access is a union of role permissions/restrictions.  
  Source: `https://docs.datadoghq.com/logs/guide/logs-rbac-permissions`

### Multi-tenancy
- **Multi-organization accounts**: Datadog can manage multiple child orgs under a parent org; child orgs do not have access to each other’s data by default; users can be members of parent + multiple child orgs and switch between them.  
  Source: `https://docs.datadoghq.com/account_management/multi_organization/`
- **SSO not inherited**: SAML setup is not inherited from parent to child org; it must be configured per child org.  
  Source: `https://docs.datadoghq.com/account_management/multi_organization/`
- **Custom subdomains** can help ensure links route to the correct org context for multi-org users.  
  Source: `https://docs.datadoghq.com/account_management/multi_organization/`

### AuthZ granularity (what you can scope)
- Logs permissions include **index-level scoping** for some actions (for example, `logs_write_exclusion_filters` can be global or restricted to a subset of indexes; `logs_read_archives` can be scoped to specific archives).  
  Source: `https://docs.datadoghq.com/logs/guide/logs-rbac-permissions`

## Sumo Logic

### Authentication (AuthN)
- **SAML 2.0 SSO** is supported, with optional on-demand user creation and SP-initiated login configuration.  
  Source: `https://help.sumologic.com/docs/manage/security/saml/set-up-saml`
- Sumo Logic highlights operational caveats, including that **access keys are not controlled by SAML** and that SAML “does not provide a deprovisioning mechanism.”  
  Source: `https://help.sumologic.com/docs/manage/security/saml/set-up-saml`

### Authorization (AuthZ)
- **RBAC**: permissions are assigned to roles (not users directly). A role includes **capabilities** and a **role search filter** that limits what data users can view.  
  Source: `https://www.sumologic.com/help/docs/manage/users-roles/roles/role-based-access-control/`
- **Multiple roles**: when a user has multiple roles, Sumo Logic combines role filters using **OR**, and “the least restrictive filter takes precedence.”  
  Source: `https://www.sumologic.com/help/docs/manage/users-roles/roles/role-based-access-control/`

### Multi-tenancy
- Sumo Logic treats accounts as **organizations**, and supports **multi-account access** (switching between orgs) when authenticating via username/password (not SAML).  
  Source: `https://service.sumologic.com/help/docs/manage/users-roles/users/multi-account-access/`
- Sumo Logic recommends **custom subdomains** as the best approach for accessing multiple accounts, and notes multi-account access is not available when logged in via SAML because it can’t verify access to other accounts.  
  Source: `https://service.sumologic.com/help/docs/manage/users-roles/users/multi-account-access/`

## Parity checklist (what Gastrolog may need to match)

Use the sections above as a **requirements matrix**, not an integration guide. At a high level, peers converge on:

- **AuthN breadth**: local users plus enterprise SSO (SAML/OIDC/LDAP patterns), optional MFA via IdP, and machine/API identity (tokens, keys, certs) where products split “human login” from “automation.”
- **AuthZ as roles + unions**: most stacks assign **roles**; effective permission is often the **union** of roles—design Gastrolog so “extra role” cannot accidentally **remove** access unless that is an explicit product decision.
- **Layered enforcement**: separate **data-plane** limits (indexes, streams, queries, DLS/FLS, restriction filters) from **control-plane** limits (pipelines, retention, admin APIs) and from **object** ACLs (dashboards, saved searches, knowledge objects). Gastrolog likely needs clear semantics for each layer.
- **Multi-tenancy story**: first-class **tenant/workspace/org** boundaries (Elastic Spaces, OpenSearch tenants, Grafana orgs, Datadog orgs) vs **convention-only** isolation (Splunk-style indexes + roles). Decide what Gastrolog’s primitive is and how data and saved objects hang off it.
- **Break-glass operations**: documented expectation of a **local or password-capable** admin path when SSO is strict or misconfigured (see Graylog’s local admin guidance and Datadog’s multi-org SAML notes as examples of why this matters).

This list is intentionally incomplete; the per-product subsections are the source of truth for granularity and vendor-specific caveats.

