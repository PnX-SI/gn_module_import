import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute } from "@angular/router";

import { DataService } from '../../services/data.service'
import { CsvExportService } from "../../services/csv-export.service";

@Component({
    selector: 'pnx-import-errors',
    templateUrl: 'import_report.component.html',
    styleUrls: ["import_report.component.scss"],

})

export class ImportReportComponent implements OnInit, OnDestroy {
    private sub: any;
    public import: any;
    public formatedErrors: string;
    public expansionPanelHeight: string = "60px";
    public validBbox: Object;
    public validData: Array<Object>;
    public fields: Array<Object>;
    public nomenclature: Array<Object>;
    constructor(private _dataService: DataService, private _csvExport: CsvExportService, private _activedRoute: ActivatedRoute) { }


    ngOnInit() {

        this.sub = this._activedRoute.params.subscribe(params => {
            const idImport: number = params["id_import"];
            this._dataService.getOneImport(idImport).subscribe(data => {
                if (data.import_count) {
                    // Load additionnal data if imported data
                    this.loadValidData(idImport);
                    const fieldMapping = data.id_field_mapping;
                    this.loadMapping(fieldMapping);
                    this.loadNomenclature(idImport, fieldMapping);
                }
                this.import = data;
            })
        })
    }

    /** Gets the validBbox and validData (info about observations)
     * @param {string}  idImport - id of the import to get the info from
     */
    loadValidData(idImport: number) {
        this._dataService.getValidData(idImport
        ).subscribe(data => {
            this.validBbox = data.valid_bbox;
            this.validData = data.valid_data;
        })
    }

    loadMapping(idMapping: number) {
        this._dataService.getMappingFields(idMapping)
            .subscribe(data => {
                this.fields = data
            })
    }

    loadNomenclature(idImport: number, idMapping: number) {
        this._dataService.getNomencInfo(idImport, idMapping)
            .subscribe(data => {
                console.log(data)
                this.nomenclature = data
            })
    }

    ngOnDestroy() {
        this.sub.unsubscribe();
    }
}