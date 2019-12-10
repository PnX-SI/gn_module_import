import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';
import { StepsService } from '../steps.service';

@Component({
	selector: 'footer-stepper',
	styleUrls: [ 'footer-stepper.component.scss' ],
	templateUrl: 'footer-stepper.component.html'
})
export class FooterStepperComponent implements OnInit {
	public srid: any;
	public columns;
	public IMPORT_CONFIG = ModuleConfig;
	isLinear: boolean = true;
	contentMappingInfo: any;
	table_name: any;
	selected_columns: any;
	added_columns: any;
	importId: any;
	stepId: any;
	dataStep1: any;
	dataStep2: any;

	constructor(
		private _router: Router,
		private _ds: DataService,
		private stepService: StepsService,
		private toastr: ToastrService
	) {}

	ngOnInit() {}

	cancelImport() {
		this._ds.cancelImport(this.importId).subscribe(
			() => {
				this.stepService.resetStepoer();
				this._router.navigate([ `${this.IMPORT_CONFIG.MODULE_URL}` ]);
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					if ((error.status = 400)) {
						this._router.navigate([ `${this.IMPORT_CONFIG.MODULE_URL}` ]);
					}
					// show error message if other server error
					this.toastr.error(error.error.message);
				}
			}
		);
	}

	onImportList() {
		this.stepService.resetStepoer();
		this._router.navigate([ `${this.IMPORT_CONFIG.MODULE_URL}` ]);
	}
}
