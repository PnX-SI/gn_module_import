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
	public isFullError = false;
	public userFieldMapping;
	public newMapping: boolean = false;
	public id_mapping;

	step2_btn: boolean = false;
	contentMappingInfo: any;
	isUserError: boolean = false;
	stepForm: FormGroup;

	@Input() columns: any;
	@Input() srid: any;
	@Input() importId: any;

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

		for (let col of this.IMPORT_CONFIG.MAPPING_DATA_FRONTEND) {
			for (let field of col.fields) {
				if (field.required) {
					this.syntheseForm.addControl(field.name, new FormControl('', Validators.required));
				} else {
					this.syntheseForm.addControl(field.name, new FormControl(''));
				}
			}
		}

		this.getMappingList();
		this.onSelectUserMapping();
	}

	onMapping(value) {
		this.isUploading = true;
		delete value.stepper;
		this._ds.postMapping(value, this.importId, this.id_mapping, this.srid).subscribe(
			(res) => {			
				this.contentMappingInfo = res.content_mapping_info;
				this.isUploading = false;
				this.n_error_lines = res['n_user_errors'];
				this.dataCleaningErrors = res['user_error_details'];
				this.contentMappingInfo.map((content) => {
					content.isCollapsed = true;
				});
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
				}
			}
		);
	}

	onNextStep() {
		this.stepService.nextStep(this.syntheseForm, 'two', this.contentMappingInfo);
	}

	getMappingList() {
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
					this.getSelectedMapping(id_mapping);
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
					this.toastr.error(error.error);
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
		}
	}
}
