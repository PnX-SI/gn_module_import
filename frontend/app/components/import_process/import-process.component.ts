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
	public selectFieldMappingForm: FormGroup;
	public syntheseForm: FormGroup;
	public synColumnNames;
	public importId;
	public isUserError: boolean = false;
	public userErrors;
	public columns;
	public n_error_lines;
	public isErrorButtonClicked: boolean = false;
	public dataCleaningErrors;
	public isFullError = false;
	public userFieldMapping;
	public mappingFieldNameResponse;
	public newMapping: boolean = false;
	public id_mapping;
    public user_srid;
    public step3Response;

	//public impatient: boolean = false;
	step1_btn: boolean = true;
	step2_btn: boolean = false;

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
			stepper: [ null, Validators.required ] // hack for material 2.0.0 beta
		});

		this.selectFieldMappingForm = this._fb.group({
			fieldMapping: [ null ],
			mappingName: [ '' ]
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
				this.isUploading = false;
				this.importId = this.uploadResponse.importId;
				this.columns = this.uploadResponse.columns.map((col) => {
					return {
						id: col,
						selected: false
					};
				});
				this.getFieldMappings();
				this.onSelectUserMapping();
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
		this.user_srid = this.uploadForm.get('srid').value;
		this._ds.postMapping(value, this.importId, this.id_mapping, this.user_srid).subscribe(
			(res) => {
				this.mappingResponse = res;
				this.isUploading = false;
				this.n_error_lines = this.mappingResponse['n_user_errors'];
				this.dataCleaningErrors = this.mappingResponse['user_error_details'];
				this.step2_btn = true;
				this.isFullErrorCheck(this.mappingResponse['n_table_rows'], this.n_error_lines);
                stepper.next();
                console.log(this.mappingResponse);
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
			}
		);
    }
    

    onStep3() {
        this._ds.postMetaToStep3(this.importId, this.id_mapping, this.mappingResponse['selected_columns'], this.mappingResponse['table_name']).subscribe(
			(res) => {
                this.step3Response = res;
                console.log(this.step3Response);
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

	getFieldMappings() {
		// get list of all declared dataset of the user
		this._ds.getFieldMappings().subscribe(
			(result) => {
				this.userFieldMapping = result;
			},
			(err) => {
				console.log(err);
				if (err.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if user does not have any declared dataset
					this.toastr.error(err.error);
				}
			}
		);
	}

	getUsersMapping(id_mapping) {
		this.id_mapping = id_mapping;
		// get list of all declared dataset of the user
		this._ds.getMappingFields(id_mapping).subscribe(
			(mappingFields) => {
				if (mappingFields[0] != 'empty') {
					for (let field of mappingFields) {
						this.syntheseForm.get(field['target_field']).setValue(field['source_field']);
					}
					this.getSelectedOptions();
				} else {
					Object.keys(this.syntheseForm.controls).forEach((key) => {
						this.syntheseForm.get(key).setValue('');
					});
				}
			},
			(err) => {
				if (err.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if user does not have any declared dataset
					this.toastr.error(err.error);
				}
			}
		);
	}

	onSelectUserMapping(): void {
		this.selectFieldMappingForm.get('fieldMapping').valueChanges.subscribe(
			(id_mapping) => {
				if (id_mapping) {
					this.getUsersMapping(id_mapping);
				} else {
					Object.keys(this.syntheseForm.controls).forEach((key) => {
						this.syntheseForm.get(key).setValue('');
					});
					this.getSelectedOptions();
				}
			},
			(err) => {
				if (err.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if user does not have any declared dataset
					this.toastr.error(err.error);
				}
			}
		);
	}

	onMappingFieldName(value) {
		this._ds.postMappingFieldName(value).subscribe(
			(res) => {
				this.mappingFieldNameResponse = res;
				this.newMapping = false;
				this.getFieldMappings();
				this.selectFieldMappingForm.controls['fieldMapping'].setValue(this.mappingFieldNameResponse);
				this.selectFieldMappingForm.controls['mappingName'].setValue('');
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if other server error
					this.isUserError = true;
					this.userErrors = error.error;
					this.toastr.error(this.userErrors);
				}
			}
		);
	}

	createMapping() {
		this.selectFieldMappingForm.reset();
		this.newMapping = true;
	}

	cancelMapping() {
		this.newMapping = false;
		this.selectFieldMappingForm.controls['mappingName'].setValue('');
	}

	onFinalImport() {
		//this.impatient = true;
	}

	onImportList() {
		this._router.navigate([ `${this.IMPORT_CONFIG.MODULE_URL}` ]);
	}

	onStepBack(stepper: MatStepper) {
		stepper.previous();
	}

	Formlistener() {
		this.uploadForm.valueChanges.subscribe(() => {
			if (
				this.uploadForm.get('file').valid &&
				this.uploadForm.get('encodage').valid &&
				this.uploadForm.get('separator').valid &&
				this.uploadForm.get('separator').valid
			)
				this.step1_btn = false;
		});
	}

	onSelect() {
		this.getSelectedOptions();
	}

	getSelectedOptions() {
		let formValues = this.syntheseForm.value;
		this.columns.map((col) => {
			if (formValues) {
				if (Object.values(formValues).includes(col.id)) {
					col.selected = true;
				} else {
					col.selected = false;
				}
			}
		});
	}

	ErrorButtonClicked() {
		this.isErrorButtonClicked = !this.isErrorButtonClicked;
	}

	isFullErrorCheck(n_table_rows, n_errors) {
		if (n_table_rows == n_errors) {
			this.isFullError = true;
		}
	}
}
