import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { ToastrService } from 'ngx-toastr';
import { DataService } from '../../services/data.service';
import { ModuleConfig } from '../../module.config';
import { StepsService } from '../import_process/steps.service';

@Component({
	selector: 'pnx-import',
	styleUrls: [ 'import.component.scss' ],
	templateUrl: 'import.component.html'
})
export class ImportComponent implements OnInit {
	public deletedStep1;
	public history;
	public empty: boolean = false;
	public config = ModuleConfig;

	constructor(
		private _ds: DataService,
		private _router: Router,
		private stepService: StepsService,
		private toastr: ToastrService
	) {}

	ngOnInit() {
		this.onImportList();
		this.onDelete_aborted_step1();
		// faire promesse pour structurer le déroulement de ces 2 appels
	}

	private onImportList() {
		this._ds.getImportList().subscribe(
			(res) => {
				this.history = res.history;
				this.empty = res.empty;
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if other server error
					this.toastr.error(error.error);
				}
			}
		);
	}

	private onDelete_aborted_step1() {
		this._ds.delete_aborted_step1().subscribe(
			(res) => {
				this.deletedStep1 = res;
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if other server error
					this.toastr.error(error.error);
				}
			}
		);
	}

	onFinishImport(row) {
		console.log('import', row);
		this._router.navigate([ `${ModuleConfig.MODULE_URL}/process` ], {
			queryParams: { datasetId: row.id_dataset }
		});
		this.stepService.goToStep(row.step)
	}
}
