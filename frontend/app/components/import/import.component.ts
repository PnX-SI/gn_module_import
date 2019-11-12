import { Component, OnInit } from '@angular/core';
import { ToastrService } from 'ngx-toastr';
import { DataService } from '../../services/data.service';
import { ModuleConfig } from '../../module.config';
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

	constructor(private _ds: DataService, private toastr: ToastrService) {}

	ngOnInit() {
		this.onImportList();
		this.onDelete_aborted_step1();
	}

	private onImportList() {
		this._ds.getImportList().subscribe(
			(res) => {
                this.history = res.history;
                console.log(this.history);
				this.empty = res.empty;
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

	onFinishImport(row){
		console.log('import',row);
    }
    

    onCSV(row) {
        console.log(row);
        this.historyId = row.id_import;
        this._ds.checkInvalid(this.historyId).subscribe(
            (res) => {
                this.n_invalid = res;
                console.log(this.n_invalid)
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
                let filename = 'invalid_data.csv'
                this._ds.getCSV(this.historyId).subscribe(
                    (res) => {
                        saveAs(res, filename);
                    },
                    (error) => {
                        if (error.statusText === 'Unknown Error')
                            this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
                        else
                            this.toastr.error('INTERNAL SERVER ERROR when downloading csv file');
                    }
                );
            }
        );

	
}
