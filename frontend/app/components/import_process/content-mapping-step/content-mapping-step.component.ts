import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { FormControl, FormGroup, FormBuilder } from '@angular/forms';
import { StepsService, Step3Data, Step4Data } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ContentMappingService } from '../../../services/mappings/content-mapping.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';

@Component({
	selector: 'content-mapping-step',
	styleUrls: [ 'content-mapping-step.component.scss' ],
	templateUrl: 'content-mapping-step.component.html'
})
export class ContentMappingStepComponent implements OnInit, OnChanges {

	public isCollapsed = false;
	public userContentMapping;
	public newMapping: boolean = false;
	public id_mapping;
	public columns;
	public spinner: boolean = false;
	contentTargetForm: FormGroup;
	showForm: boolean = false;
	contentMapRes: any;
	stepData: Step3Data;
    contentMappingObj = [];


	constructor(
        private stepService: StepsService, 
        private _fb: FormBuilder,
        private _ds: DataService,
        private _cm: ContentMappingService,
		private toastr: ToastrService,
		private _router: Router
        ) {}


	ngOnInit() {
		this.stepData = this.stepService.getStepData(3);

		this._cm.contentMappingForm = this._fb.group({
			contentMapping: [ null ],
			mappingName: [ '' ]
		});
		this.contentTargetForm = this._fb.group({});
		this._cm.getMappingNamesList('content', this.stepData.importId);

		if (this._cm.contentMappingInfo) {
			if (this._cm.contentMappingInfo.length > 0) {
				console.log(this._cm.contentMappingInfo);
				this.stepData.contentMappingInfo = this._cm.contentMappingInfo;
			}
		}

		this._cm.onMappingName(this._cm.contentMappingForm, this.contentTargetForm, this.stepData.table_name, this.contentMappingObj, this.stepData.contentMappingInfo);
		this._cm.generateContentForm(this.stepData.contentMappingInfo, this.contentTargetForm, this.stepData.table_name, this.contentMappingObj);

		if (this.stepData.contentMappingInfo) {
			this.stepData.contentMappingInfo.forEach((ele) => {
				ele['nomenc_values_def'].forEach((nomenc) => {
					this.contentTargetForm.addControl(nomenc.id, new FormControl(''));
				});
			});
			this.showForm = true;
		}
		if (this.stepData.id_content_mapping) {
			this._cm.fillMapping(this.stepData.id_content_mapping, this.contentTargetForm);
		}
	}


	/*
	getNomencInf(table_name, obj) {
		this._ds.getNomencInfo(table_name, obj).subscribe(
			(res) => {		
                this.contentMappingInfo = res['content_mapping_info'];
                console.log(this.contentMappingInfo);
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
        );
	}
	*/


	onSelectChange(selectedVal, group) {
		this.stepData.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr) {
				ele.user_values.values = ele.user_values.values.filter((value) => {
					return value.id != selectedVal.id;
				});
			}
		});
		this._cm.onMappingName(this._cm.contentMappingForm, this.contentTargetForm, this.stepData.table_name, this.contentMappingObj, this.stepData.contentMappingInfo);

	}


	onSelectDelete(deletedVal, group) {
		this.stepData.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr) {
				let temp_array = ele.user_values.values;
				temp_array.push(deletedVal);
				ele.user_values.values = temp_array.slice(0);
			}
		});
		this._cm.onMappingName(this._cm.contentMappingForm, this.contentTargetForm, this.stepData.table_name, this.contentMappingObj, this.stepData.contentMappingInfo);

	}


	onStepBack() {
		this._router.navigate([ `${ModuleConfig.MODULE_URL}/process/step/2` ]);
	}


	onContentMapping(value) {
		// post content mapping form values and fill t_mapping_values table
		this.id_mapping = this._cm.contentMappingForm.get('contentMapping').value;
		this.spinner = true;
		this._ds
			.postContentMap(
				value,
				this.stepData.table_name,
				this.stepData.selected_columns,
				this.stepData.importId,
				this.id_mapping
			)
			.subscribe(
				(res) => {
					this.contentMapRes = res;
					let step4Data: Step4Data = {
						importId: this.stepData.importId,
						selected_columns: this.stepData.selected_columns,
						added_columns: this.stepData.added_columns
					};
				
					let step3Data: Step3Data = this.stepData;
					step3Data.id_content_mapping = this.id_mapping;
					this.stepService.setStepData(3, step3Data);
					this.stepService.setStepData(4, step4Data);

					this._router.navigate([ `${ModuleConfig.MODULE_URL}/process/step/4` ]);
					this.spinner = false;
				},
				(error) => {
					this.spinner = false;
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
    
}
