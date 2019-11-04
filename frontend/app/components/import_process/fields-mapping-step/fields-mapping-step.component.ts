import { Component, OnInit, Input } from '@angular/core';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';
import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';
import { StepsService } from '../steps.service';

@Component({
	selector: 'fields-mapping-step',
	styleUrls: [ 'fields-mapping-step.component.scss' ],
	templateUrl: 'fields-mapping-step.component.html'
})

export class FieldsMappingStepComponent implements OnInit {

	public isUploading: boolean = false;
	public IMPORT_CONFIG = ModuleConfig;
	public selectFieldMappingForm: FormGroup;
	public syntheseForm: FormGroup;
	public n_error_lines;
	public isErrorButtonClicked: boolean = false;
	public dataCleaningErrors;
	public isFullError: boolean = true;
	public userFieldMapping;
	public newMapping: boolean = false;
    public id_mapping;
    public step3Response;
    public table_name;
    public selected_columns;
    public added_columns;

	step2_btn: boolean = false;
	contentMappingInfo: any;
	isUserError: boolean = false;


	@Input() columns: any;
	@Input() srid: any;
	@Input() importId: any;
    mappingRes: any;
    bibRes: any;

	constructor(
		private _ds: DataService,
		private toastr: ToastrService,
		private _fb: FormBuilder,
		private stepService: StepsService
	) {}

	ngOnInit() {
		this.selectFieldMappingForm = this._fb.group({
			fieldMapping: [ null ],
			mappingName: [ '' ]
		});

		this.syntheseForm = this._fb.group({});

        
        this._ds.getBibFields().subscribe(
            (res) => {
                this.bibRes = res;
                for (let theme of this.bibRes) {
                    for (let field of theme.fields) {
                        if (field.required) {
                            this.syntheseForm.addControl(field.name_field, new FormControl('', Validators.required));
                        } else {
                            this.syntheseForm.addControl(field.name_field, new FormControl(''));
                        }
                    }
                }
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
        )

		this.getMappingList();
        this.onSelectUserMapping();
	}

	onMapping(value) {
		this.isUploading = true;
		delete value.stepper;
		this._ds.postMapping(value, this.importId, this.id_mapping, this.srid).subscribe(
			(res) => {		
                this.mappingRes =res;
                console.log(this.mappingRes);
				this.isUploading = false;
				this.n_error_lines = res['n_user_errors'];
                this.dataCleaningErrors = res['user_error_details'];
                this.table_name = this.mappingRes['table_name'];
                this.selected_columns = JSON.stringify(this.mappingRes['selected_columns']);
                this.added_columns = this.mappingRes['added_columns'];
				this.step2_btn = true;
                this.isFullErrorCheck(res['n_table_rows'], this.n_error_lines);
			},
			(error) => {
				this.isUploading = false;
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if other server error
					this.isUserError = true;
                    this.isUserError = error.error;
                    console.log(error);
                    this.toastr.error(error.error.message);
				}
			}
		);
    }
    
    onNextStep()  {
        this._ds.postMetaToStep3(this.importId, this.id_mapping, this.mappingRes['selected_columns'], this.mappingRes['table_name']).subscribe(
			(res) => {
                this.step3Response = res;
                this.contentMappingInfo = res.content_mapping_info;
                this.contentMappingInfo.map((content) => {
					content.isCollapsed = true;
                });
                this.step3Response['added_columns'] = this.added_columns;
                this.stepService.nextStep(this.syntheseForm, 'two', this.step3Response);
                console.log(this.step3Response);
			},
			(error) => {
				this.isUploading = false;
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

	getMappingList() {
		// get list of all declared dataset of the user
		this._ds.getFieldMappings().subscribe(
			(result) => {
				this.userFieldMapping = result;
			},
			(error) => {
				console.log(error);
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.log(error);
                    this.toastr.error(error.error.message);
				}
			}
		);
	}

	getSelectedMapping(id_mapping) {
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
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.log(error);
                    this.toastr.error(error.error.message);
				}
			}
		);
	}

	onSelectUserMapping(): void {
		this.selectFieldMappingForm.get('fieldMapping').valueChanges.subscribe(
			(id_mapping) => {
				if (id_mapping) {
					this.getSelectedMapping(id_mapping);
				} else {
					Object.keys(this.syntheseForm.controls).forEach((key) => {
						this.syntheseForm.get(key).setValue('');
					});
					this.getSelectedOptions();
				}
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.log(error);
                    this.toastr.error(error.error.message);
				}
			}
		);
	}

	onMappingFieldName(value) {
		this._ds.postMappingFieldName(value).subscribe(
			(res) => {
				this.newMapping = false;
				this.getMappingList();
				this.selectFieldMappingForm.controls['fieldMapping'].setValue(res);
				this.selectFieldMappingForm.controls['mappingName'].setValue('');
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.log(error);
                    this.toastr.error(error.error.message);
				}
			}
		);
	}

	createMapping() {
		this.selectFieldMappingForm.reset();
		this.newMapping = true;
	}

	onCancelMapping() {
		this.newMapping = false;
		this.selectFieldMappingForm.controls['mappingName'].setValue('');
	}

	onStepBack() {
		this.stepService.previousStep();
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
		} else {
            this.isFullError = false;
        }
	}
}
