import generalConcepts from './general-concepts.md?raw';
import ingestion from './ingestion.md?raw';
import ingesterSyslog from './ingester-syslog.md?raw';
import ingesterHttp from './ingester-http.md?raw';
import ingesterRelp from './ingester-relp.md?raw';
import ingesterTail from './ingester-tail.md?raw';
import ingesterDocker from './ingester-docker.md?raw';
import ingesterChatterbox from './ingester-chatterbox.md?raw';
import ingesterOtlp from './ingester-otlp.md?raw';
import ingesterFluentfwd from './ingester-fluentfwd.md?raw';
import ingesterKafka from './ingester-kafka.md?raw';
import digesters from './digesters.md?raw';
import digesterLevel from './digester-level.md?raw';
import digesterTimestamp from './digester-timestamp.md?raw';
import routing from './routing.md?raw';
import storage from './storage.md?raw';
import storageFile from './storage-file.md?raw';
import storageMemory from './storage-memory.md?raw';
import policyRotation from './policy-rotation.md?raw';
import policyRetention from './policy-retention.md?raw';
import indexers from './indexers.md?raw';
import queryEngine from './query-engine.md?raw';
import queryLanguage from './query-language.md?raw';
import explain from './explain.md?raw';
import savedQueries from './saved-queries.md?raw';
import security from './security.md?raw';
import userManagement from './user-management.md?raw';
import certificates from './certificates.md?raw';
import inspector from './inspector.md?raw';
import inspectorStores from './inspector-stores.md?raw';
import inspectorIngesters from './inspector-ingesters.md?raw';
import inspectorJobs from './inspector-jobs.md?raw';
import serviceSettings from './service-settings.md?raw';
import settingsOverview from './settings.md?raw';
import recipes from './recipes.md?raw';
import recipeDockerMtls from './recipe-docker-mtls.md?raw';
import recipeRsyslog from './recipe-rsyslog.md?raw';
import recipePromtail from './recipe-promtail.md?raw';
import recipeJournald from './recipe-journald.md?raw';
import about from './about.md?raw';

export interface HelpTopic {
  id: string;
  title: string;
  content: string;
  children?: HelpTopic[];
}

// Topics ordered to follow the data flow through the system:
// Ingest → Digest → Route → Store → Index → Search
export const helpTopics: HelpTopic[] = [
  { id: 'general-concepts', title: 'General Concepts', content: generalConcepts },
  {
    id: 'ingestion', title: 'Ingestion', content: ingestion,
    children: [
      { id: 'ingester-syslog', title: 'Syslog', content: ingesterSyslog },
      { id: 'ingester-http', title: 'HTTP (Loki)', content: ingesterHttp },
      { id: 'ingester-relp', title: 'RELP', content: ingesterRelp },
      { id: 'ingester-otlp', title: 'OTLP', content: ingesterOtlp },
      { id: 'ingester-fluentfwd', title: 'Fluent Forward', content: ingesterFluentfwd },
      { id: 'ingester-kafka', title: 'Kafka', content: ingesterKafka },
      { id: 'ingester-tail', title: 'Tail', content: ingesterTail },
      { id: 'ingester-docker', title: 'Docker', content: ingesterDocker },
      { id: 'ingester-chatterbox', title: 'Chatterbox', content: ingesterChatterbox },
    ],
  },
  {
    id: 'digesters', title: 'Digestion', content: digesters,
    children: [
      { id: 'digester-level', title: 'Level', content: digesterLevel },
      { id: 'digester-timestamp', title: 'Timestamp', content: digesterTimestamp },
    ],
  },
  { id: 'routing', title: 'Filtering', content: routing },
  {
    id: 'storage', title: 'Storage', content: storage,
    children: [
      { id: 'storage-file', title: 'File Store', content: storageFile },
      { id: 'storage-memory', title: 'Memory Store', content: storageMemory },
      { id: 'policy-rotation', title: 'Rotation Policies', content: policyRotation },
      { id: 'policy-retention', title: 'Retention Policies', content: policyRetention },
    ],
  },
  { id: 'indexers', title: 'Indexing', content: indexers },
  {
    id: 'query-engine', title: 'Searching', content: queryEngine,
    children: [
      { id: 'query-language', title: 'Query Language', content: queryLanguage },
      { id: 'saved-queries', title: 'Saved Queries', content: savedQueries },
      { id: 'explain', title: 'Explain', content: explain },
    ],
  },
  {
    id: 'security', title: 'Security', content: security,
    children: [
      { id: 'user-management', title: 'Users & Authentication', content: userManagement },
      { id: 'certificates', title: 'Certificates', content: certificates },
    ],
  },
  {
    id: 'inspector', title: 'Inspector', content: inspector,
    children: [
      { id: 'inspector-stores', title: 'Stores', content: inspectorStores },
      { id: 'inspector-ingesters', title: 'Ingesters', content: inspectorIngesters },
      { id: 'inspector-jobs', title: 'Jobs', content: inspectorJobs },
    ],
  },
  {
    id: 'settings', title: 'Settings', content: settingsOverview,
    children: [
      { id: 'service-settings', title: 'Service', content: serviceSettings },
    ],
  },
  {
    id: 'recipes', title: 'Recipes', content: recipes,
    children: [
      { id: 'recipe-docker-mtls', title: 'Docker with mTLS', content: recipeDockerMtls },
      { id: 'recipe-rsyslog', title: 'rsyslog', content: recipeRsyslog },
      { id: 'recipe-promtail', title: 'Promtail / Grafana Agent', content: recipePromtail },
      { id: 'recipe-journald', title: 'systemd journal', content: recipeJournald },
    ],
  },
  { id: 'about', title: 'About', content: about },
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
