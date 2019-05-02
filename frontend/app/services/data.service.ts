import { OnInit } from '@angular/core';
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { AppConfig } from '@geonature_config/app.config';
import { CommonService } from '@geonature_common/service/common.service';
import { ModuleConfig } from "../module.config";


@Injectable()
export class DataService {

    fd: any;
    public IMPORT_CONFIG = ModuleConfig;

    constructor(
        private _http: HttpClient, 
        private _commonService: CommonService
    ) {}

    getImportList() {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}`);
    }

    postUserFile(selectedFile) {
        const urlStatus = `${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/uploads`;
        this.fd = new FormData();
        this.fd.append('File', selectedFile, selectedFile.name);
        return this._http.post<any>(urlStatus, this.fd);
    }

    postDataset(datasetId:number) {
        const url = `${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/init`;
        return this._http.post<any>(url,datasetId);
    }

    getUserDatasets() {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/datasets`);
    }

    deleteImport(importId:number) {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/ImportDelete/${importId}`);
    }

}