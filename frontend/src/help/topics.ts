import generalConcepts from './general-concepts.md?raw';
import queryLanguage from './query-language.md?raw';
import queryEngine from './query-engine.md?raw';
import explain from './explain.md?raw';
import storageEngines from './storage-engines.md?raw';
import storageFile from './storage-file.md?raw';
import storageMemory from './storage-memory.md?raw';
import routing from './routing.md?raw';
import policyRotation from './policy-rotation.md?raw';
import policyRetention from './policy-retention.md?raw';
import ingesters from './ingesters.md?raw';
import ingesterSyslog from './ingester-syslog.md?raw';
import ingesterHttp from './ingester-http.md?raw';
import ingesterRelp from './ingester-relp.md?raw';
import ingesterTail from './ingester-tail.md?raw';
import ingesterDocker from './ingester-docker.md?raw';
import ingesterChatterbox from './ingester-chatterbox.md?raw';
import digesters from './digesters.md?raw';
import digesterLevel from './digester-level.md?raw';
import digesterTimestamp from './digester-timestamp.md?raw';
import indexers from './indexers.md?raw';
import inspector from './inspector.md?raw';
import inspectorStores from './inspector-stores.md?raw';
import inspectorIngesters from './inspector-ingesters.md?raw';
import inspectorJobs from './inspector-jobs.md?raw';
import userManagement from './user-management.md?raw';

export interface HelpTopic {
  id: string;
  title: string;
  content: string;
  children?: HelpTopic[];
}

export const helpTopics: HelpTopic[] = [
  { id: 'general-concepts', title: 'General Concepts', content: generalConcepts },
  {
    id: 'ingesters', title: 'Ingestion', content: ingesters,
    children: [
      { id: 'ingester-syslog', title: 'Syslog', content: ingesterSyslog },
      { id: 'ingester-http', title: 'HTTP (Loki)', content: ingesterHttp },
      { id: 'ingester-relp', title: 'RELP', content: ingesterRelp },
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
  {
    id: 'storage-engines', title: 'Storage', content: storageEngines,
    children: [
      { id: 'routing', title: 'Routing', content: routing },
      { id: 'policy-rotation', title: 'Rotation', content: policyRotation },
      { id: 'policy-retention', title: 'Retention', content: policyRetention },
      { id: 'storage-file', title: 'File Store', content: storageFile },
      { id: 'storage-memory', title: 'Memory Store', content: storageMemory },
    ],
  },
  { id: 'indexers', title: 'Indexing', content: indexers },
  {
    id: 'query-engine', title: 'Searching', content: queryEngine,
    children: [
      { id: 'query-language', title: 'Query Language', content: queryLanguage },
      { id: 'explain', title: 'Explain', content: explain },
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
  { id: 'user-management', title: 'Users & Security', content: userManagement },
];

export function findTopic(id: string): HelpTopic | undefined {
  return findTopicIn(helpTopics, id);
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
