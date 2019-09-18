import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { DataService } from '../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../module.config';
import { MatStepper } from '@angular/material';
import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';

@Component({
	selector: 'pnx-import-process',
	styleUrls: [ 'import-process.component.scss' ],
	templateUrl: 'import-process.component.html'
})
export class ImportProcessComponent implements OnInit {
	public fileName;
	private uploadResponse;
	public cancelResponse;
	public mappingResponse;
	public isUploading: Boolean = false;
	public IMPORT_CONFIG = ModuleConfig;
	public uploadForm: FormGroup;
	public syntheseForm: FormGroup;
	public synColumnNames;
	private importId;
	public isUserError: boolean = false;
	public userErrors;
	public columns;

	public impatient: boolean = false;
	step1_btn: boolean = true;

	constructor(
		private _router: Router,
		private _activatedRoute: ActivatedRoute,
		private _ds: DataService,
		private toastr: ToastrService,
		private _fb: FormBuilder
	) {}

	ngOnInit() {
		this.uploadForm = this._fb.group({
			file: [ null, Validators.required ],
			encodage: [ null, Validators.required ],
			srid: [ null, Validators.required ],
			separator: [ null, Validators.required ],
			stepper: [ null, Validators.required ] // hack for matrial 2.0.0 beta
		});
		this.syntheseForm = this._fb.group({
			stepper: [ null, Validators.required ]
		});
		for (let col of this.IMPORT_CONFIG.MAPPING_DATA_FRONTEND) {
			for (let field of col.fields) {
				if (field.required) {
					this.syntheseForm.addControl(field.name, new FormControl('', Validators.required));
				} else {
					this.syntheseForm.addControl(field.name, new FormControl(''));
				}
			}
		}
		this.Formlistener();
	}

	onFileSelected(event) {
		this.uploadForm.patchValue({
			file: <File>event.target.files[0]
		});
		this.fileName = this.uploadForm.get('file').value.name;
	}

	cancelImport() {
		this._ds.cancelImport(this.importId).subscribe(
			(res) => {
				this.cancelResponse = res;
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
					this.toastr.error(error.error);
				}
			}
		);
	}

	onUpload(value, stepper: MatStepper) {
		this.isUploading = true;
		this.isUserError = false;
		delete value.stepper;
		this._ds.postUserFile(value, this._activatedRoute.snapshot.queryParams['datasetId'], this.importId).subscribe(
			(res) => {
				this.uploadResponse = res;
				this.IMPORT_CONFIG;
				this.uploadForm.removeControl('stepper');
				stepper.next();
				//this.getSynColumnNames();
				this.isUploading = false;
				this.importId = this.uploadResponse.importId;
				this.columns = this.uploadResponse.columns;
			},
			(error) => {
				this.isUploading = false;
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					if (error.status == 500) {
						// show error message if other server error
						console.error('erreur 500 attention');
						this.toastr.error(error.error);
					}
					if (error.status == 400) {
						this.isUserError = true;
						this.userErrors = error.error;
					}
					if (error.status == 403) {
						this.toastr.error(error.error);
					}
				}
			}
		);
	}

	onMapping(value, stepper: MatStepper) {
		this.isUploading = true;
		this._ds.postMapping(value, this.importId).subscribe(
			(res) => {
				this.mappingResponse = res;
				this.isUploading = false;
				stepper.next();
			},
			(error) => {
				this.isUploading = false;
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if other server error
					this.isUserError = true;
					this.userErrors = error.error;
				}
            },
            () => {
                console.log(this.mappingResponse)
            }
		);
	}

	onFinalStep() {
		/*
    this._ds.postMapping(value, this.importId).subscribe(
      res => {
        this.mappingResponse = res as JSON;
      },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          // show error message if other server error
          this.toastr.error(error.error.message);
        }
      },
      () => {
        console.log(this.mappingResponse);
      }
    );
    */
	}

	onFinalImport() {
		this.impatient = true;
	}

	onImportList() {
		this._router.navigate([ `${this.IMPORT_CONFIG.MODULE_URL}` ]);
	}

	onStepBack(stepper: MatStepper) {
		stepper.previous();
	}

	Formlistener() {
		this.uploadForm.valueChanges.subscribe((result) => {
			if (
				this.uploadForm.get('file').valid &&
				this.uploadForm.get('encodage').valid &&
				this.uploadForm.get('separator').valid &&
				this.uploadForm.get('separator').valid
			)
				this.step1_btn = false;
		});
		this.syntheseForm.valueChanges.subscribe((result) => {
			this.columns = this.columns.filter(function(ele) {
				return !Object.values(result).includes(ele);
			});
		});
	}

	/*les colonnes sont récupérées à partir de la conf !!!
	getSynColumnNames() {
		// attention
		this._ds.getSynColumnNames().subscribe(
			(res) => {
				this.synColumnNames = res;
				for (let col of this.synColumnNames) {
					if (col.is_nullable === 'NO') {
						this.syntheseForm.addControl(col.column_name, new FormControl('', Validators.required));
					} else {
						this.syntheseForm.addControl(col.column_name, new FormControl(''));
					}
				}
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
*/
}
