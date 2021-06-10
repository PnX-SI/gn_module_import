import { Step } from "./enums.model";
import { Cruved } from "./cruved.model";

export interface Mapping {
    id_mapping: number;
    mapping_label: string;
    mapping_type: string;
    active: boolean;
    is_public: boolean;
    cruved: Cruved;
}

export interface MappingField {
    id_match_fields: number;
    id_mapping: number;
    source_field: string;
    target_field: string;
    is_selected: boolean;
    is_added: boolean;
}

export interface MappingContent {
    id_match_values: number;
    id_mapping: number;
    target_field_name: string;
    source_value: string;
    id_target_value: number; // better to have nomenclature label?
}
