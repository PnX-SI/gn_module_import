import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { StepsService } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';
import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';
import { MatStepper } from '@angular/material';

@Component({
	selector: 'import-step',
	styleUrls: [ 'import-step.component.scss' ],
	templateUrl: 'import-step.component.html'
})
export class ImportStepComponent implements OnInit, OnChanges {

	public isCollapsed = false;
    @Input() selected_columns: any;
    @Input() added_columns: any;
    @Input() table_name: any;
    @Input() importId: any;
    importDataRes: any;
	importForm: FormGroup;


	constructor(
        private stepService: StepsService, 
        private _ds: DataService,
        private toastr: ToastrService,
        private _fb: FormBuilder,
        ) {}


	ngOnInit() {
        this.importForm = this._fb.group({});
	}


	onStepBack() {
		this.stepService.previousStep();
    }

    onImport() {
        this._ds.importData(this.selected_columns, this.added_columns, this.table_name, this.importId).subscribe(
            (res) => {		
                this.importDataRes = res;
                console.log(this.importDataRes);
            },
            (error) => {
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