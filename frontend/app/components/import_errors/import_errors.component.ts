import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute } from "@angular/router";

import { DataService } from '../../services/data.service'

@Component({
    selector: 'pnx-import-errors',
    templateUrl: 'import_errors.component.html',
    styleUrls: ["import_errors.component.scss"],

})

export class ImportErrorsComponent implements OnInit, OnDestroy {
    private sub: any;
    public import: any;
    public formatedErrors: string;
    public expansionPanelHeight: string = "60px";
    constructor(private _dataService: DataService, private _activedRoute: ActivatedRoute) { }

    ngOnInit() {

        this.sub = this._activedRoute.params.subscribe(params => {
            this._dataService.getOneImport(
                params["id_import"]
            ).subscribe(data => {
                console.log(data);

                this.import = data;
            })

        })
    }

    ngOnDestroy() {
        this.sub.unsubscribe();
    }
}