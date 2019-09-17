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

	cancelImport(importId: number) {
		return this._http.get<any>(`${urlApi}/cancel_import/${importId}`);
	}

	getSynColumnNames() {
		return this._http.get<any>(`${urlApi}/syntheseInfo`);
	}

	postMapping(value, importId: number) {
		const urlMapping = `${urlApi}/mapping/${importId}`;
		return this._http.post<any>(urlMapping, value);
	}

	delete_aborted_step1() {
		return this._http.get<any>(`${urlApi}/delete_step1`);
	}
}
