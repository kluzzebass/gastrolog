export interface IngesterParamsFormProps {
  ingesterType: string;
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
  ingesterId?: string;
  ingesterNodeId?: string;
}

export interface SubFormProps {
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
  defaults: Record<string, string>;
  ingesterId?: string;
  ingesterNodeId?: string;
}
