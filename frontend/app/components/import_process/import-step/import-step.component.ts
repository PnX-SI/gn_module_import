import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { StepsService, Step4Data } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';
import * as _ from 'lodash';

@Component({
	selector: 'import-step',
	styleUrls: [ 'import-step.component.scss' ],
	templateUrl: 'import-step.component.html'
})
export class ImportStepComponent implements OnInit {
	public isCollapsed = false;
	importDataRes: any;
	validData: any;
	total_columns: any;
	columns: any[] = [];
	rows: any[] = [];
	tableReady: boolean = false;
	stepData: Step4Data;

	constructor(
		private stepService: StepsService,
		private _ds: DataService,
		private _router: Router,
		private toastr: ToastrService
	) {}

	ngOnInit() {
		this.stepData = this.stepService.getStepData(4);
		this.getValidData();
	}

	onStepBack() {
		this._router.navigate([ `${ModuleConfig.MODULE_URL}/process/step/3` ]);
	}

	onImport() {
		this._ds.importData(this.stepData.importId, this.total_columns).subscribe(
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
					this.toastr.error(error.error.message + ' = ' + error.error.details);
				}
			}
		);
	}

	getValidData() {
		this._ds
			.getValidData(this.stepData.importId, this.stepData.selected_columns, this.stepData.added_columns)
			.subscribe(
				(res) => {
					this.validData = res.valid_data;
					this.total_columns = res['total_columns'];

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
