import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { DataService } from '../../../services/data.service';
import { FieldMappingService } from '../../../services/mappings/field-mapping.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';
import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';
import { StepsService, Step2Data, Step3Data, Step1Data } from '../steps.service';

@Component({
	selector: 'fields-mapping-step',
	styleUrls: [ 'fields-mapping-step.component.scss' ],
	templateUrl: 'fields-mapping-step.component.html'
})
export class FieldsMappingStepComponent implements OnInit {
	
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
	columns: any;
	mappingRes: any;
	bibRes: any;
	stepData: Step2Data;

	constructor(
        private _ds: DataService,
        private _fm: FieldMappingService,
		private toastr: ToastrService,
		private _fb: FormBuilder,
		private stepService: StepsService,
		private _router: Router
	) {}


	ngOnInit() {
        this.stepData = this.stepService.getStepData(2);
        this._fm.fieldMappingForm = this._fb.group({
            fieldMapping: [ null ],
            mappingName: [ '' ]
        });
        this.syntheseForm = this._fb.group({});
        this.generateSyntheseForm();
		this._fm.getMappingNamesList('field', this.stepData.importId);
		
		this._fm.onMappingName(this._fm.fieldMappingForm, this.syntheseForm);

	
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
                if (this.stepData.id_field_mapping) {       
                    this._fm.fieldMappingForm.controls['fieldMapping'].setValue(this.stepData.id_field_mapping);
                    this._fm.fillMapping(this.stepData.id_field_mapping, this.syntheseForm);
				}
				this.onFormMappingChange();
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
		this._ds.postMapping(value, this.stepData.importId, id_mapping, this.stepData.srid).subscribe(
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
					if (error.status == 400) this.isUserError = true;
					this.toastr.error(error.error.message);
				}
			}
		);
	}


	onNextStep() {
		if (this.mappingRes == undefined) {
			this.toastr.error("Veuillez valider le mapping avant de passer à l'étape suivante");
		}
		this._ds
			.postMetaToStep3(
				this.stepData.importId,
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
					let step3data: Step3Data = {
						contentMappingInfo: this.step3Response.content_mapping_info,
						selected_columns: this.step3Response.selected_columns,
						table_name: this.step3Response.table_name,
						importId: this.stepData.importId,
						added_columns: this.step3Response.added_columns
					};
					let step2data: Step2Data = {
					importId: this.stepData.importId,
					srid: this.stepData.srid,
					id_field_mapping: this._fm.id_mapping,
					mappingRes: this.mappingRes
					};

					this.stepService.setStepData(3, step3data);
					this.stepService.setStepData(2, step2data);
					this._router.navigate([ `${ModuleConfig.MODULE_URL}/process/step/3` ]);
					//this.stepService.nextStep(this.syntheseForm, 'two', this.step3Response);
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
		this.syntheseForm.valueChanges.subscribe((dd) => {
			console.log("dd",dd);
			
			if (this.step2_btn) {
				this.mappingRes = null;
				this.step2_btn = false;
			}


				
		/**if (this.stepData.mappingRes) {
			this.mappingRes = this.stepData.mappingRes;
			this.table_name = this.mappingRes['table_name'];
			this.n_error_lines = this.mappingRes['n_user_errors'];
	
			this.isFullErrorCheck(this.mappingRes['n_table_rows'], this.n_error_lines);
		}**/
		});
	}

    
	onStepBack() {
		this._router.navigate([ `${ModuleConfig.MODULE_URL}/process/step/1` ]);
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
