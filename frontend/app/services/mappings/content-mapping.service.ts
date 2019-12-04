import { Injectable } from '@angular/core';
import { FormGroup, FormControl } from '@angular/forms';
import { DataService } from '../data.service';
import { ToastrService } from 'ngx-toastr';


@Injectable()
export class ContentMappingService {

    public contentMappingForm: FormGroup;
    public userContentMappings;
	public newMapping: boolean = false;
    public id_mapping;
    public contentMappingInfo;
	public showForm: boolean = false;


    constructor(
        private _ds: DataService, 
        private toastr: ToastrService
    ) {}


    getMappingNamesList(mapping_type, importId) {
        // get list of existing content mapping in the select
		this._ds.getMappings(mapping_type, importId).subscribe(
			(result) => {
                console.log(result);
                this.userContentMappings = result['mappings'];
			},
			(error) => {
				console.log(error);
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
		this.contentMappingForm.reset();
		this.newMapping = true;
	}


	cancelMapping() {
		this.newMapping = false;
		this.contentMappingForm.controls['mappingName'].setValue('');
    }


    saveMappingName(value, importId, targetForm) {
        // save new mapping in bib_mapping
        // then select the mapping name in the select
        let mappingType = 'CONTENT';
		this._ds.postMappingName(value, mappingType).subscribe(
			(res) => {
                console.log(res);
				this.newMapping = false;
				this.getMappingNamesList(mappingType, importId);
				this.contentMappingForm.controls['contentMapping'].setValue(res);
                this.contentMappingForm.controls['mappingName'].setValue('');
                //this.enableMapping(targetForm);
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


    onMappingName(mappingForm, targetFormName, table_name, obj, contentMappingInfo): void {
        //this.generateContentForm(contentMappingInfo, targetFormName, table_name, obj)
		mappingForm.get('contentMapping').valueChanges.subscribe(
			(id_mapping) => {
                this.generateContentForm(contentMappingInfo, targetFormName, table_name, obj)
                console.log('truc')
				if (id_mapping) {
					this.fillMapping(id_mapping, targetFormName);
				} else {
                    targetFormName.reset();
                }
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


	getNomencInf(table_name, obj, contentTargetForm) {
		this._ds.getNomencInfo(table_name, obj).subscribe(
			(res) => {		
                this.contentMappingInfo = res['content_mapping_info'];
                console.log(this.contentMappingInfo);
                this.contentMappingInfo.forEach(
                    (ele) => {
                        ele['nomenc_values_def'].forEach(
                            (nomenc) => {
                                contentTargetForm.addControl(nomenc.id, new FormControl(''));
                            });
                    });
                this.showForm = true;
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


    generateContentForm(contentMappingInfo, contentTargetForm, table_name, contentMappingObj) {
		console.log('test');
        if (contentMappingInfo) {
            this.getNomencInf(table_name, contentMappingObj, contentTargetForm);
            console.log(contentMappingInfo);

        }
    }
    

    fillMapping(id_mapping, targetFormName) {
		this.id_mapping = id_mapping;
		this._ds.getMappingContents(id_mapping).subscribe(
			(mappingContents) => {
                targetFormName.reset();
				if (mappingContents[0] != 'empty') {
					for (let content of mappingContents) {
                        let arrayVal: any = [];
                        for (let val of content) {
                            if (val['source_value'] != '') {
                                arrayVal.push({value : val['source_value']});
                            }
                        }
                        targetFormName.get(String(content[0]['id_target_value'])).setValue(arrayVal);
                    }                    
				} else {
                    targetFormName.reset();
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


    /*
	enableMapping(targetForm) {
		Object.keys(targetForm.controls).forEach((key) => {
			targetForm.controls[key].enable();
		});
    }
    */

    
}