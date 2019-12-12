import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { ToastrService } from 'ngx-toastr';
import { DataService } from '../../services/data.service';
import { ModuleConfig } from '../../module.config';
import { StepsService, Step2Data, Step1Data, Step3Data } from '../import_process/steps.service';
import { saveAs } from 'file-saver';

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
	historyId: any;
	n_invalid: any;
	csvDownloadResp: any;

	constructor(
		private _ds: DataService,
		private _router: Router,
		private stepService: StepsService,
		private toastr: ToastrService
	) {}

	ngOnInit() {
		this.onImportList();
		this.onDelete_aborted_step1();
	}

	private onImportList() {
		this._ds.getImportList().subscribe(
			(res) => {
				this.history = res.history;
				this.empty = res.empty;
				console.log('history', this.history);
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if other server error
					this.toastr.error(error.error.message);
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
					this.toastr.error(error.error.message);
				}
			}
		);
	}

	onFinishImport(row) {
		console.log('import', row);
		localStorage.setItem('startPorcess', JSON.stringify(true));
		let dataStep1 :Step1Data = {
			importId: row.id_import,
			datasetId: row.id_dataset,
			formData: {
				fileName: 'fileName.' + row.format_source_file, // fileName = import table ?
				encoding: row.encoding,
				srid: row.srid,
				separator: ";"/// separator to convert
			}
		};
		let dataStep2 :Step2Data = {
			importId: row.id_import,
			srid: row.srid,
		}
		switch (row.step) {
			case 2: {
				localStorage.setItem('step1Data', JSON.stringify(dataStep1));
				localStorage.setItem('step2Data', JSON.stringify(dataStep2));
				break;
			}
			case 3: {
				dataStep2.id_field_mapping = row.id_field_mapping
				let dataStep3 :Step3Data = {
					importId: row.id_import,

				}
				localStorage.setItem('step2Data', JSON.stringify(dataStep2));
				localStorage.setItem('step3Data', JSON.stringify(dataStep3));
				break;
			}
			case 4: {
				//localStorage.setItem('step4Data', JSON.stringify(data));
				break;
			}
		}
		this._router.navigate([ `${ModuleConfig.MODULE_URL}/process/step/${row.step}` ]);
	}

	onCSV(row) {
		console.log(row);
		this.historyId = row.id_import;
		this._ds.checkInvalid(this.historyId).subscribe(
			(res) => {
				this.n_invalid = res;
				console.log(this.n_invalid);
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if other server error
					console.log(error);
					this.toastr.error(error.error);
				}
			},
			() => {
				let filename = 'invalid_data.csv';
				this._ds.getCSV(this.historyId).subscribe(
					(res) => {
						saveAs(res, filename);
					},
					(error) => {
						if (error.statusText === 'Unknown Error')
							this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
						else this.toastr.error('INTERNAL SERVER ERROR when downloading csv file');
					}
				);
			}
		);
	}
}
