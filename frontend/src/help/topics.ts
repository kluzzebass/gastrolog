export interface HelpTopic {
  id: string;
  title: string;
  /** Lazy content loader — call to get the markdown string. */
  load: () => Promise<string>;
  children?: HelpTopic[];
}

function md(loader: () => Promise<{ default: string }>) {
  return () => loader().then((m) => m.default);
}

// Topics ordered to follow the data flow through the system:
// Ingest → Digest → Route → Store → Index → Search
export const helpTopics: HelpTopic[] = [
  { id: 'general-concepts', title: 'General Concepts', load: md(() => import('./general-concepts.md?raw')) },
  {
    id: 'ingestion', title: 'Ingestion', load: md(() => import('./ingestion.md?raw')),
    children: [
      { id: 'ingester-syslog', title: 'Syslog', load: md(() => import('./ingester-syslog.md?raw')) },
      { id: 'ingester-http', title: 'HTTP (Loki)', load: md(() => import('./ingester-http.md?raw')) },
      { id: 'ingester-relp', title: 'RELP', load: md(() => import('./ingester-relp.md?raw')) },
      { id: 'ingester-otlp', title: 'OTLP', load: md(() => import('./ingester-otlp.md?raw')) },
      { id: 'ingester-fluentfwd', title: 'Fluent Forward', load: md(() => import('./ingester-fluentfwd.md?raw')) },
      { id: 'ingester-kafka', title: 'Kafka', load: md(() => import('./ingester-kafka.md?raw')) },
      { id: 'ingester-tail', title: 'Tail', load: md(() => import('./ingester-tail.md?raw')) },
      { id: 'ingester-docker', title: 'Docker', load: md(() => import('./ingester-docker.md?raw')) },
      { id: 'ingester-metrics', title: 'Metrics', load: md(() => import('./ingester-metrics.md?raw')) },
      { id: 'ingester-chatterbox', title: 'Chatterbox', load: md(() => import('./ingester-chatterbox.md?raw')) },
    ],
  },
  {
    id: 'digesters', title: 'Digestion', load: md(() => import('./digesters.md?raw')),
    children: [
      { id: 'digester-level', title: 'Level', load: md(() => import('./digester-level.md?raw')) },
      { id: 'digester-timestamp', title: 'Timestamp', load: md(() => import('./digester-timestamp.md?raw')) },
    ],
  },
  { id: 'routing', title: 'Filtering', load: md(() => import('./routing.md?raw')) },
  {
    id: 'storage', title: 'Storage', load: md(() => import('./storage.md?raw')),
    children: [
      { id: 'storage-file', title: 'File Store', load: md(() => import('./storage-file.md?raw')) },
      { id: 'storage-memory', title: 'Memory Store', load: md(() => import('./storage-memory.md?raw')) },
      { id: 'policy-rotation', title: 'Rotation Policies', load: md(() => import('./policy-rotation.md?raw')) },
      { id: 'policy-retention', title: 'Retention Policies', load: md(() => import('./policy-retention.md?raw')) },
    ],
  },
  { id: 'indexers', title: 'Indexing', load: md(() => import('./indexers.md?raw')) },
  {
    id: 'query-engine', title: 'Searching', load: md(() => import('./query-engine.md?raw')),
    children: [
      { id: 'query-language', title: 'Query Language', load: md(() => import('./query-language.md?raw')) },
      { id: 'pipeline', title: 'Pipeline Queries', load: md(() => import('./pipeline.md?raw')) },
      { id: 'saved-queries', title: 'Saved Queries', load: md(() => import('./saved-queries.md?raw')) },
      { id: 'explain', title: 'Explain', load: md(() => import('./explain.md?raw')) },
    ],
  },
  {
    id: 'security', title: 'Security', load: md(() => import('./security.md?raw')),
    children: [
      { id: 'user-management', title: 'Users & Authentication', load: md(() => import('./user-management.md?raw')) },
      { id: 'certificates', title: 'Certificates', load: md(() => import('./certificates.md?raw')) },
    ],
  },
  {
    id: 'inspector', title: 'Inspector', load: md(() => import('./inspector.md?raw')),
    children: [
      { id: 'inspector-stores', title: 'Stores', load: md(() => import('./inspector-stores.md?raw')) },
      { id: 'inspector-ingesters', title: 'Ingesters', load: md(() => import('./inspector-ingesters.md?raw')) },
      { id: 'inspector-jobs', title: 'Jobs', load: md(() => import('./inspector-jobs.md?raw')) },
    ],
  },
  {
    id: 'settings', title: 'Settings', load: md(() => import('./settings.md?raw')),
    children: [
      { id: 'service-settings', title: 'Service', load: md(() => import('./service-settings.md?raw')) },
    ],
  },
  {
    id: 'recipes', title: 'Recipes', load: md(() => import('./recipes.md?raw')),
    children: [
      { id: 'recipe-docker-mtls', title: 'Docker with mTLS', load: md(() => import('./recipe-docker-mtls.md?raw')) },
      { id: 'recipe-rsyslog', title: 'rsyslog', load: md(() => import('./recipe-rsyslog.md?raw')) },
      { id: 'recipe-promtail', title: 'Promtail / Grafana Agent', load: md(() => import('./recipe-promtail.md?raw')) },
      { id: 'recipe-journald', title: 'systemd journal', load: md(() => import('./recipe-journald.md?raw')) },
    ],
  },
  { id: 'about', title: 'About', load: md(() => import('./about.md?raw')) },
];

/**
 * Aliases for topic IDs that were moved or merged. Settings components
 * reference these via helpTopicId — the alias ensures they still resolve.
 */
const topicAliases: Record<string, string> = {
  'ingesters': 'ingestion',
  'storage-engines': 'storage',
};

/** Resolve an alias to its canonical topic ID. */
export function resolveTopicId(id: string): string {
  return topicAliases[id] ?? id;
}

export function findTopic(id: string): HelpTopic | undefined {
  return findTopicIn(helpTopics, resolveTopicId(id));
}

function findTopicIn(topics: HelpTopic[], id: string): HelpTopic | undefined {
  for (const t of topics) {
    if (t.id === id) return t;
    if (t.children) {
      const found = findTopicIn(t.children, id);
      if (found) return found;
    }
  }
  return undefined;
}

/** Flatten all topics into a single array for search. */
export function allTopics(): HelpTopic[] {
  const result: HelpTopic[] = [];
  function collect(topics: HelpTopic[]) {
    for (const t of topics) {
      result.push(t);
      if (t.children) collect(t.children);
    }
  }
  collect(helpTopics);
  return result;
}
