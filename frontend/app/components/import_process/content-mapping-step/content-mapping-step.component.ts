import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { FormControl, FormGroup, FormBuilder } from '@angular/forms';
import { StepsService } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';

@Component({
	selector: 'content-mapping-step',
	styleUrls: [ 'content-mapping-step.component.scss' ],
	templateUrl: 'content-mapping-step.component.html'
})
export class ContentMappingStepComponent implements OnInit, OnChanges {

    public isCollapsed = false;
    public selectContentMappingForm: FormGroup;
	public userContentMapping;
	public newMapping: boolean = false;
    public id_mapping;
    public columns;

	public spinner: boolean = false;
    @Input() contentMappingInfo: any;
    @Input() selected_columns: any;
    @Input() table_name: any;
    @Input() importId: any;
	contentForm: FormGroup;
	showForm: boolean = false;
    contentMapRes: any;
    


	constructor(
        private stepService: StepsService, 
        private _fb: FormBuilder,
        private _ds: DataService,
        private toastr: ToastrService
        ) {}


	ngOnInit() {
        this.selectContentMappingForm = this._fb.group({
			contentMapping: [ null ],
			mappingName: [ '' ]
		});
        this.contentForm = this._fb.group({});
        this.getMappingList('content');
        this.onSelectUserMapping();
	}

	ngOnChanges() {
		if (this.contentMappingInfo) {
			this.contentMappingInfo.forEach((ele) => {
				ele['nomenc_values_def'].forEach((nomenc) => {
                    this.contentForm.addControl(nomenc.id, new FormControl(''));
                });
			});
			this.showForm = true;
        }
	}

	onSelectChange(selectedVal, group) {
		this.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr)
			{
				ele.user_values.values = ele.user_values.values.filter(
					(value) => {
					return value.id !=selectedVal.id;
				});
			}
		})
	}

<<<<<<< HEAD

	onSelectDelete(deletedVal, group) {
		this.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr)
			{
                let temp_array = ele.user_values.values;
				temp_array.push(deletedVal);
				ele.user_values.values = temp_array;
=======
	onSelectDelete(deltetdVal, group) {
		this.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr)
			{
				let temp_array = ele.user_values.values;
				temp_array.push(deltetdVal);
				ele.user_values.values = temp_array.slice(0);
>>>>>>> ee5c98573737f2567e58490d2845ef22de538593
			}
		})
	}


	onStepBack() {
		this.stepService.previousStep();
    }
    

    onContentMapping(value) {
<<<<<<< HEAD
        // post content mapping form values and fill t_mapping_values table
        console.log(this.contentForm);
        this.id_mapping = this.selectContentMappingForm.get('contentMapping').value;
        this._ds.postContentMap(value, this.table_name, this.selected_columns, this.importId, this.id_mapping).subscribe(
=======
		this.spinner = true;
        this._ds.postContentMap(value, this.table_name, this.selected_columns).subscribe(
>>>>>>> ee5c98573737f2567e58490d2845ef22de538593
            (res) => {		
                this.contentMapRes = res;
				this.stepService.nextStep(this.contentForm, 'three');
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
		this._ds.getMappings(mapping_type, this.importId).subscribe(
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
                                arrayVal.push({value : val['source_value']});
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
    
}