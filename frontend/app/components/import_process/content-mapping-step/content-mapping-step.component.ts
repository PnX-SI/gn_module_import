import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { FormControl, FormGroup, FormBuilder } from '@angular/forms';
import { StepsService, Step3Data, Step4Data } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';

@Component({
	selector: 'content-mapping-step',
	styleUrls: [ 'content-mapping-step.component.scss' ],
	templateUrl: 'content-mapping-step.component.html'
})
export class ContentMappingStepComponent implements OnInit {
	
	public isCollapsed = false;
	public userContentMapping;
	public newMapping: boolean = false;
	public id_mapping;
	public columns;
	public spinner: boolean = false;
	selectContentMappingForm: FormGroup;
	contentForm: FormGroup;
	showForm: boolean = false;
	contentMapRes: any;
	stepData: Step3Data;

	constructor(
		private stepService: StepsService,
		private _fb: FormBuilder,
		private _ds: DataService,
		private toastr: ToastrService,
		private _router: Router
	) {}

	ngOnInit() {
		this.stepData = this.stepService.getStepData(3);

		this.selectContentMappingForm = this._fb.group({
			contentMapping: [ null ],
			mappingName: [ '' ]
		});
		this.contentForm = this._fb.group({});
		this.getMappingList('content');
		this.onSelectUserMapping();
		if (this.stepData.contentMappingInfo) {
			this.stepData.contentMappingInfo.forEach((ele) => {
				ele['nomenc_values_def'].forEach((nomenc) => {
					this.contentForm.addControl(nomenc.id, new FormControl(''));
				});
			});
			this.showForm = true;
		}
		if (this.stepData.id_content_mapping) {
			console.log('this.id_field_mapping 3', this.stepData);

			this.selectContentMappingForm.controls['contentMapping'].setValue(this.stepData.id_content_mapping);
			this.getSelectedMapping(this.stepData.id_content_mapping);
		}
	}

	onSelectChange(selectedVal, group) {
		this.stepData.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr) {
				ele.user_values.values = ele.user_values.values.filter((value) => {
					return value.id != selectedVal.id;
				});
			}
		});
		this.onSelectUserMapping();
	}

	onSelectDelete(deletedVal, group) {
		this.stepData.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr) {
				let temp_array = ele.user_values.values;
				temp_array.push(deletedVal);
				ele.user_values.values = temp_array.slice(0);
			}
		});
		this.onSelectUserMapping();
	}

	onStepBack() {
		this._router.navigate([ `${ModuleConfig.MODULE_URL}/process/step/2` ]);
	}

	onContentMapping(value) {
		// post content mapping form values and fill t_mapping_values table
		this.id_mapping = this.selectContentMappingForm.get('contentMapping').value;
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

	getMappingList(mapping_type) {
		// get list of existing content mapping in the select
		this._ds.getMappings(mapping_type, this.stepData.importId).subscribe(
			(result) => {
				this.userContentMapping = result['mappings'];
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

	onMappingContentName(value) {
		// save new mapping in bib_mapping
		// then select the mapping name in the select
		let mappingType = 'CONTENT';
		this._ds.postMappingName(value, mappingType).subscribe(
			(res) => {
				console.log(res);
				this.newMapping = false;
				this.getMappingList(mappingType);
				this.selectContentMappingForm.controls['contentMapping'].setValue(res);
				this.selectContentMappingForm.controls['mappingName'].setValue('');
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.log(error);
					this.toastr.error(error.error);
				}
			}
		);
	}

	createMapping() {
		// show input for typing mapping name
		// and deselect previously selected mapping
		this.selectContentMappingForm.reset();
		this.newMapping = true;
	}

	onCancelMapping() {
		// show input for typing mapping name
		// and deselect previously selected mapping
		this.newMapping = false;
		this.selectContentMappingForm.controls['mappingName'].setValue('');
	}

	getSelectedMapping(id_mapping) {
		this.id_mapping = id_mapping;
		this._ds.getMappingContents(id_mapping).subscribe(
			(mappingContents) => {
				this.contentForm.reset();
				if (mappingContents[0] != 'empty') {
					for (let content of mappingContents) {
						let arrayVal: any = [];
						for (let val of content) {
							if (val['source_value'] != '') {
								arrayVal.push({ value: val['source_value'] });
							}
						}
						this.contentForm.get(String(content[0]['id_target_value'])).setValue(arrayVal);
					}
				} else {
					this.contentForm.reset();
				}
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					this.toastr.error(error.error.message);
				}
			}
		);
	}

	onSelectUserMapping(): void {
		this.selectContentMappingForm.get('contentMapping').valueChanges.subscribe(
			(id_mapping) => {
				if (id_mapping) {
					this.getSelectedMapping(id_mapping);
				} else {
					this.contentForm.reset();
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

	onSelect() {
		this.getSelectedOptions();
	}

	getSelectedOptions() {
		let formValues = this.contentForm.value;
		if (this.id_mapping == undefined) {
			this.toastr.warning("Vous devez d'abord créer ou sélectionner un mapping");
		} else {
			this.columns.map((col) => {
				if (formValues) {
					if (Object.values(formValues).includes(col.id)) {
						col.selected = false;
					} else {
						col.selected = false;
					}
				}
			});
		}
	}
}
