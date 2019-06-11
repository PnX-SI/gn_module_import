import { OnInit } from '@angular/core';
import { Injectable } from '@angular/core';
import { HttpClient,HttpHeaders } from '@angular/common/http';
import { AppConfig } from '@geonature_config/app.config';
import { CommonService } from '@geonature_common/service/common.service';
import { ModuleConfig } from "../module.config";

const HttpUploadOptions = {
    headers: new HttpHeaders({ "Accept": "application/json" })
  }

const HttpFormOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'x-www-form-urlencoded'})
  }

@Injectable()
export class DataService {

    private fd: FormData;
    private IMPORT_CONFIG = ModuleConfig;

    constructor(
        private _http: HttpClient, 
        private _commonService: CommonService
    ) {}


    getImportList() {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}`);
    }


    postUserFile(value,datasetId,importId) {
        console.log(value);
        const urlStatus = `${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/uploads`;
        this.fd = new FormData();
        this.fd.append('File', value.file, value.file['name']);
        this.fd.append('encodage', value.encodage);
        this.fd.append('srid', value.srid);
        this.fd.append('separator', value.separator);
        this.fd.append('datasetId', datasetId);
        this.fd.append('importId', importId);
        console.log(this.fd);
        return this._http.post<any>(urlStatus, this.fd, HttpUploadOptions);
    }


    getUserDatasets() {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/datasets`);
    }

    
    cancelImport(importId:number) {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/cancel_import/${importId}`);
    }

    getSynColumnNames() {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/syntheseInfo`);
    }

    postMapping(value, importId:number) {
        //console.log(value);
        const urlMapping = `${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/mapping/${importId}`;        
        return this._http.post<any>(urlMapping, value);
    }

    delete_aborted_step1() {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/delete_step1`);
    }

}