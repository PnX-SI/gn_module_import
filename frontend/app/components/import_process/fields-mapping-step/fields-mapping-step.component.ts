import { Component, OnInit, Input, OnChanges } from '@angular/core';
import { DataService } from '../../../services/data.service';
import { FieldMappingService } from '../../../services/mappings/field-mapping.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';
import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';
import { StepsService } from '../steps.service';


@Component({
	selector: 'fields-mapping-step',
	styleUrls: [ 'fields-mapping-step.component.scss' ],
	templateUrl: 'fields-mapping-step.component.html'
})
export class FieldsMappingStepComponent implements OnInit, OnChanges {

	public spinner: boolean = false;
	public IMPORT_CONFIG = ModuleConfig;
	public syntheseForm: FormGroup;
	public n_error_lines;
	public isErrorButtonClicked: boolean = false;
	public dataCleaningErrors;
	public isFullError: boolean = true;
	public id_mapping;
	public step3Response;
	public table_name;
	public selected_columns;
	public added_columns;

	step2_btn: boolean = false;
	contentMappingInfo: any;
	isUserError: boolean = false;
	formReady: boolean = false;

	@Input() columns: any;
	@Input() srid: any;
	@Input() importId: any;
	mappingRes: any;
	bibRes: any;

	constructor(
        private _ds: DataService,
        private _fm: FieldMappingService,
		private toastr: ToastrService,
		private _fb: FormBuilder,
		private stepService: StepsService
	) {}

	ngOnInit() {
    }


	ngOnChanges() {
		this.formReady = false;
		this._fm.fieldMappingForm = this._fb.group({
			fieldMapping: [null],
			mappingName: ['']
		});
        this.syntheseForm = this._fb.group({});
        this.generateSyntheseForm();
		this._fm.getMappingNamesList('field', this.importId);
		this._fm.onMappingName(this._fm.fieldMappingForm, this.syntheseForm);
        this.onFormMappingChange();
	}


    generateSyntheseForm() {
        this._ds.getBibFields().subscribe(
            (res) => {
                this.bibRes = res;
                const validators = [Validators.required];
                for (let theme of this.bibRes) {
                    for (let field of theme.fields) {
                        if (field.required) {
                            this.syntheseForm.addControl(field.name_field, new FormControl({value:'', disabled: true}, validators));
                            this.syntheseForm.get(field.name_field).setValidators([Validators.required]);
                        } else {
                            this.syntheseForm.addControl(field.name_field, new FormControl({value:'', disabled: true}));
                        }
                    }
                }
                this.formReady = true;
            },
            (error) => {
                if (error.statusText === 'Unknown Error') {
                    // show error message if no connexion
                    this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
                } else {
                    // show error message if other server error
                    console.error(error);
                    this.toastr.error(error.error.message);
                }
            }
        );
    }


	onDataCleaning(value, id_mapping) {
        this.spinner = true;
        console.log(id_mapping);
		this._ds.postMapping(value, this.importId, id_mapping, this.srid).subscribe(
			(res) => {
				this.mappingRes = res;
				this.spinner = false;
				this.n_error_lines = res['n_user_errors'];
				this.dataCleaningErrors = res['user_error_details'];
				this.table_name = this.mappingRes['table_name'];
				this.selected_columns = JSON.stringify(this.mappingRes['selected_columns']);
				this.added_columns = this.mappingRes['added_columns'];
				this.step2_btn = true;
				this.isFullErrorCheck(res['n_table_rows'], this.n_error_lines);
			},
			(error) => {
				this.spinner = false;
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if other server error
					if (error.status == 400)
                        this.isUserError = true;
                    this.toastr.error(error.error.message);
				}
			}
		);
	}


	onNextStep() {
		console.log(this.mappingRes);
		if (this.mappingRes == undefined) {
			this.toastr.error('Veuillez valider le mapping avant de passer à l\'étape suivante');
		}
		this._ds
			.postMetaToStep3(
				this.importId,
				this._fm.id_mapping,
				this.mappingRes['selected_columns'],
				this.mappingRes['table_name']
			)
			.subscribe(
				(res) => {
					this.step3Response = res;
					this.contentMappingInfo = res.content_mapping_info;
					this.contentMappingInfo.map((content) => {
						content.isCollapsed = true;
					});
					this.step3Response['added_columns'] = this.added_columns;
					this.stepService.nextStep(this.syntheseForm, 'two', this.step3Response);
				},
				(error) => {
					this.spinner = false;
					if (error.statusText === 'Unknown Error') {
						// show error message if no connexion
						this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
					} else {
						// show error message if other server error
						console.error(error);
						this.toastr.error(error.error.message);
					}
				}
			);
	}    


	onFormMappingChange() {
		this.syntheseForm.valueChanges.subscribe(() => {
			if (this.step2_btn) {
				this.mappingRes = null;
				this.step2_btn = false;
				this.stepService.resetCurrentStep('two');
			}
		});
	}

    
	onStepBack() {
		this.stepService.previousStep();
	}


	ErrorButtonClicked() {
		this.isErrorButtonClicked = !this.isErrorButtonClicked;
	}


	isFullErrorCheck(n_table_rows, n_errors) {
		if (n_table_rows == n_errors) {
			this.isFullError = true;
			this.toastr.warning('Attention, toutes les lignes ont des erreurs');
		} else {
			this.isFullError = false;
		}
    }

}
