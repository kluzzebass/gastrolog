import queryLanguage from './query-language.md?raw';

export interface HelpTopic {
  id: string;
  title: string;
  content: string;
}

export const helpTopics: HelpTopic[] = [
  { id: 'query-language', title: 'Query Language', content: queryLanguage },
];

export function findTopic(id: string): HelpTopic | undefined {
  return helpTopics.find(t => t.id === id);
}
