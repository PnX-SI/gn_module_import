import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { StepsService } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { Router } from '@angular/router';
import { ModuleConfig } from '../../../module.config';
import * as _ from 'lodash';

@Component({
    selector: 'import-step',
    styleUrls: ['import-step.component.scss'],
    templateUrl: 'import-step.component.html'
})
export class ImportStepComponent implements OnInit, OnChanges {
    public isCollapsed = false;
    @Input() selected_columns: any;
    @Input() added_columns: any;
    @Input() importId: any;
    @Input() stepId: any;
    importDataRes: any;
    validData: any;
    total_columns: any;
    columns: any[] = [];
    rows: any[] = [];
    tableReady: boolean = false;
    public spinner: boolean = false;


    constructor(
        private stepService: StepsService,
        private _router: Router,
        private _ds: DataService,
        private toastr: ToastrService
    ) { }

    ngOnInit() { }

    ngOnChanges() {
        if (this.stepId == 'three') {
            this.getValidData();
        }
    }

    onStepBack() {
        this.stepService.previousStep();
    }

    onImport() {
        this.spinner = true;
        this._ds.importData(this.importId, this.total_columns).subscribe(
            (res) => {
                this.spinner = false;
                this.importDataRes = res;
                console.log(this.importDataRes);
                this._router.navigate([`${ModuleConfig.MODULE_URL}`]);
            },
            (error) => {
                this.spinner = false;
                if (error.statusText === 'Unknown Error') {
                    // show error message if no connexion
                    this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
                } else {
                    // show error message if other server error
                    console.log(error);
                    this.toastr.error(error.error.message + ' = ' + error.error.details);
                }
            }
        );
    }

    getValidData() {
        this.spinner = true;
        this._ds.getValidData(this.importId, this.selected_columns, this.added_columns).subscribe(
            (res) => {
                console.log(res);
                this.spinner = false;
                this.validData = res.valid_data;
                if (this.validData != 'no data') {
                    this.columns = [];
                    this.rows = [];
                    _.forEach(this.validData[0], (el) => {
                        let key = el.key;
                        let val = el.value;
                        this.columns.push({ name: key, prop: key });
                    });

                    _.forEach(this.validData, (data) => {
                        let obj = {};
                        _.forEach(data, (el) => {
                            let key = el.key;
                            let val = el.value;
                            obj[key] = val;
                        });
                        this.rows.push(obj);
                    });

                    this.tableReady = true;
                }
            },
            (error) => {
                this.spinner = false;
                if (error.statusText === 'Unknown Error') {
                    // show error message if no connexion
                    this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
                } else {
                    // show error message if other server error
                    console.log(error);
                    this.toastr.error(error.error.message);
                }
            }
        );
    }


}
        

    

}
