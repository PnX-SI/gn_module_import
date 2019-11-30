import { Injectable } from '@angular/core';
import { FormGroup } from '@angular/forms';
import { saveAs } from 'file-saver';
import { DataService } from './data.service';
import { ToastrService } from 'ngx-toastr';

@Injectable()
export class CsvExportService {

    historyId: any;
    n_invalid: any;
    csvDownloadResp: any;

    constructor(private _ds: DataService, private toastr: ToastrService) {}

    onCSV(id_import) {
        console.log(id_import);
        this._ds.checkInvalid(id_import).subscribe(
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
                this._ds.getCSV(id_import).subscribe(
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
