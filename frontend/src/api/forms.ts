import { apiClient } from './client';

export interface FormComponent {
  key: string;
  type: string;
  label?: string;
}

export interface FormSchema {
  id?: string;
  schemaVersion?: number;
  components?: FormComponent[];
}

export interface FormDefinition {
  formId: string;
  version: number;
  schema: FormSchema;
}

export const getForm = async (formKey: string): Promise<FormDefinition> => {
  const res = await apiClient.get<FormDefinition>(`/v2/forms/${formKey}`);
  return res.data;
};
