import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { AppConfig } from '@geonature_config/app.config';
import { ModuleConfig } from '../module.config';


const HttpUploadOptions = {
	headers: new HttpHeaders({ Accept: 'application/json' })
};
const urlApi = `${AppConfig.API_ENDPOINT}/${ModuleConfig.MODULE_URL}`;

@Injectable()
export class DataService {
    
	constructor(private _http: HttpClient) {}


	getImportList() {
		return this._http.get<any>(urlApi);
	}


	postUserFile(value, datasetId, importId) {
		const urlStatus = `${urlApi}/uploads`;
		let fd = new FormData();
		fd.append('File', value.file, value.file['name']);
		fd.append('encodage', value.encodage);
		fd.append('srid', value.srid);
		fd.append('separator', value.separator);
		fd.append('datasetId', datasetId);
		fd.append('importId', importId);
		return this._http.post<any>(urlStatus, fd, HttpUploadOptions);
	}


	getUserDatasets() {
		return this._http.get<any>(`${urlApi}/datasets`);
    }
    

	getMappings(mapping_type) {
        console.log('get mapping : ' + mapping_type);
		return this._http.get<any>(`${urlApi}/mappings/${mapping_type}`);
    }
    

    getMappingFields(id_mapping: number) {
		return this._http.get<any>(`${urlApi}/field_mappings/${id_mapping}`);
    }


    getMappingContents(id_mapping: number) {
		return this._http.get<any>(`${urlApi}/content_mappings/${id_mapping}`);
    }
    

    postMappingName(value, mappingType) {
        const urlMapping = `${urlApi}/mappingName`;
        let fd = new FormData();
        for (let key of Object.keys(value)) {
            fd.append(key, value[key]);
        }
        fd.append('mapping_type', mappingType);
        return this._http.post<any>(urlMapping, fd, HttpUploadOptions);
    }


	cancelImport(importId: number) {
		return this._http.get<any>(`${urlApi}/cancel_import/${importId}`);
	}


	getSynColumnNames() {
		return this._http.get<any>(`${urlApi}/syntheseInfo`);
    }
    

    getBibFields() {
        return this._http.get<any>(`${urlApi}/bibFields`);
    }


	postMapping(value, importId: number, id_mapping: number, user_srid) {
        const urlMapping = `${urlApi}/mapping/${importId}/${id_mapping}`;
        let fd = new FormData();
        for (let key of Object.keys(value)) {
            fd.append(key, value[key]);
        }
        fd.append('srid', user_srid);
		return this._http.post<any>(urlMapping, fd, HttpUploadOptions);
    }
    

    postContentMap(value, table_name, selected_columns, import_id, id_mapping) {
        const contentMappingUrl = `${urlApi}/contentMapping/${import_id}/${id_mapping}`;
        let fd = new FormData();
        for (let key of Object.keys(value)) {
            if (value[key] == null) {
                fd.append(key, '')
            } else {
                if (value[key].length > 1) {
                    for (let val of value[key]) {
                        if (val['value'] == undefined) {
                            fd.append(key, val);
                        } else {
                            fd.append(key, val['value']);
                        }
                    }
                } else {
                    if (value[key] != '') {
                        if (value[key][0]['value'] == undefined) {
                            fd.append(key, value[key])
                        } else {
                            fd.append(key, value[key][0]['value']);
                        }
                    } else {
                        fd.append(key, '');
                    }
                }
            }
        }
        fd.append('table_name', table_name);
        fd.append('selected_cols', JSON.stringify(selected_columns));
		return this._http.post<any>(contentMappingUrl, fd, HttpUploadOptions);
	}


	delete_aborted_step1() {
		return this._http.get<any>(`${urlApi}/delete_step1`);
    }
    
    
    postMetaToStep3(import_id, id_mapping, selected_columns, table_name) {
        let fd = new FormData();
        fd.append('import_id', import_id);
        fd.append('id_mapping', id_mapping);
        for (let key of Object.keys(selected_columns)) {
            fd.append(key, selected_columns[key]);
        }
        fd.append('table_name', table_name);
        return this._http.post<any>(`${urlApi}/postMetaToStep3`, fd, HttpUploadOptions);
    }


    importData(import_id, total_columns) {
        let fd = new FormData();
        fd.append('total_columns', JSON.stringify(total_columns));
        fd.append('import_id', import_id);
        return this._http.post<any>(`${urlApi}/importData/${import_id}`, fd, HttpUploadOptions);
    }


    getValidData(importId: number, selected_columns, added_columns) {
        let fd = new FormData();
        fd.append('selected_columns', JSON.stringify(selected_columns));
        fd.append('added_columns', JSON.stringify(added_columns));
        return this._http.post<any>(`${urlApi}/getValidData/${importId}`, fd, HttpUploadOptions);        
    }

    getCSV(importId: number) {
        return this._http.get<any>(`${urlApi}/getCSV/${importId}`);
    }

}
