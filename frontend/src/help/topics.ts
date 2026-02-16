import generalConcepts from './general-concepts.md?raw';
import queryLanguage from './query-language.md?raw';
import queryEngine from './query-engine.md?raw';
import storageEngines from './storage-engines.md?raw';
import ingesters from './ingesters.md?raw';
import indexers from './indexers.md?raw';
import policies from './policies.md?raw';
import userManagement from './user-management.md?raw';
import architecture from './architecture.md?raw';

export interface HelpTopic {
  id: string;
  title: string;
  content: string;
}

export const helpTopics: HelpTopic[] = [
  { id: 'general-concepts', title: 'General Concepts', content: generalConcepts },
  { id: 'query-language', title: 'Query Language', content: queryLanguage },
  { id: 'query-engine', title: 'Query Engine', content: queryEngine },
  { id: 'storage-engines', title: 'Storage Engines', content: storageEngines },
  { id: 'ingesters', title: 'Ingesters', content: ingesters },
  { id: 'indexers', title: 'Indexers', content: indexers },
  { id: 'policies', title: 'Policies', content: policies },
  { id: 'user-management', title: 'Users & Security', content: userManagement },
  { id: 'architecture', title: 'Architecture', content: architecture },
];

export function findTopic(id: string): HelpTopic | undefined {
  return helpTopics.find(t => t.id === id);
}
