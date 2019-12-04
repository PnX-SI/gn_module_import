import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { FormControl, FormGroup, FormBuilder } from '@angular/forms';
import { StepsService } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ContentMappingService } from '../../../services/mappings/content-mapping.service';
import { ToastrService } from 'ngx-toastr';

@Component({
	selector: 'content-mapping-step',
	styleUrls: [ 'content-mapping-step.component.scss' ],
	templateUrl: 'content-mapping-step.component.html'
})
export class ContentMappingStepComponent implements OnInit, OnChanges {

    public isCollapsed = false;
    public id_mapping;
    public columns;

	public spinner: boolean = false;
    @Input() contentMappingInfo: any;
    @Input() selected_columns: any;
    @Input() table_name: any;
    @Input() importId: any;
	contentTargetForm: FormGroup;
    contentMapRes: any;
    contentMappingObj = [];


	constructor(
        private stepService: StepsService, 
        private _fb: FormBuilder,
        private _ds: DataService,
        private _cm: ContentMappingService,
        private toastr: ToastrService
        ) {}


	ngOnInit() {}


	ngOnChanges() {
		if (this.contentMappingInfo) {
			for (let contentMapping of this.contentMappingInfo) {
				this.contentMappingObj[contentMapping.nomenc_synthese_name] = contentMapping.user_values.column_name;
			}
			console.log(this.contentMappingObj);
		}

        this._cm.contentMappingForm = this._fb.group({
			contentMapping: [ null ],
			mappingName: [ '' ]
		});
        this.contentTargetForm = this._fb.group({});
        this._cm.getMappingNamesList('content', this.importId);
		if (this._cm.contentMappingInfo) {
			if (this._cm.contentMappingInfo.length > 0) {
				console.log(this._cm.contentMappingInfo);
				this.contentMappingInfo = this._cm.contentMappingInfo;
			}
		}
		this._cm.onMappingName(this._cm.contentMappingForm, this.contentTargetForm, this.table_name, this.contentMappingObj, this.contentMappingInfo);


		this._cm.generateContentForm(this.contentMappingInfo, this.contentTargetForm, this.table_name, this.contentMappingObj);
		console.log(this.contentMappingInfo);
		console.log(this._cm.contentMappingInfo);
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
		console.log(selectedVal);
		console.log(group);
		this.contentMappingInfo.map(
			(ele) => {
				console.log(ele);
				if (ele.nomenc_abbr === group.nomenc_abbr) {
					ele.user_values.values = ele.user_values.values.filter(
						(value) => {
							console.log(value.id != selectedVal.id);
							return value.id != selectedVal.id;
					});
				}
		});
		this._cm.onMappingName(this.contentMappingInfo, this._cm.contentMappingForm, this.contentTargetForm, this.table_name, this.contentMappingObj);
	}


	onSelectDelete(deletedVal, group) {
		console.log(deletedVal);
		console.log(group);
		this.contentMappingInfo.map(
			(ele) => {
				console.log(ele);
				if (ele.nomenc_abbr === group.nomenc_abbr) {
					let temp_array = ele.user_values.values;
					temp_array.push(deletedVal);
					ele.user_values.values = temp_array.slice(0);
				}
			});
		//this._cm.onMappingName(this._cm.contentMappingForm, this.contentTargetForm, this.table_name, this.contentMappingObj);
	}


	onStepBack() {
		this.stepService.previousStep();
    }


    onContentMapping(value) {
        // post content mapping form values and fill t_mapping_values table
        console.log(this.contentTargetForm);
        this.id_mapping = this._cm.contentMappingForm.get('contentMapping').value;
        this.spinner = true;
        this._ds.postContentMap(value, this.table_name, this.selected_columns, this.importId, this.id_mapping).subscribe(
            (res) => {		
                this.contentMapRes = res;
				this.stepService.nextStep(this.contentTargetForm, 'three');
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


    /*
    onSelect() {
		this.getSelectedOptions();
	}


	getSelectedOptions() {
        let formValues = this.contentTargetForm.value;
        if (this.id_mapping == undefined) {
            this.toastr.warning('Vous devez d\'abord créer ou sélectionner un mapping');
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
    */
    
}