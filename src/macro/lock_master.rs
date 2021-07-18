use proc_macro::*;
use quote::quote;
use syn::PatIdent;

#[proc_macro_attribute]
pub fn locker(args: TokenStream, input: TokenStream) -> TokenStream {
    let variable_name = args.to_string();
    let mut item: syn::Item = syn::parse(input).unwrap();
    let fn_item = match &mut item {
        syn::Item::Fn(fn_item) => fn_item,
        _ => panic!("expected fn")
    };
    // Function must be async
    if fn_item.sig.asyncness.is_none() {
        panic!("function must be async");
    }
    // The first argument must be self
    if let syn::FnArg::Typed(_) = fn_item.sig.inputs[0] {
        panic!("first argument must be self")
    }
    // Check for an id argument
    let mut id: Option<PatIdent> = None;
    for arg in &fn_item.sig.inputs {
        if let syn::FnArg::Typed(typed_arg) = arg {
            let p = typed_arg.pat.clone();
            let p = *p;
            if let syn::Pat::Ident(i) = p {
                if i.ident.to_string() == variable_name {
                    id = Option::Some(i.clone());
                    break;
                }
            }
        }
    }
    if id.is_none() {
        panic!("one of the arguments must be \"{}\"", variable_name)
    }
    // Now insert the data
    let id_indent = id.unwrap().ident;
    fn_item.block.stmts.insert(0, syn::parse(quote!(let lock_guard = lock_guard.lock().await;).into()).unwrap());
    fn_item.block.stmts.insert(0, syn::parse(quote!(drop(map);).into()).unwrap());
    fn_item.block.stmts.insert(0, syn::parse(quote! {let lock_guard = map.entry(#id_indent).or_default().clone();}.into()).unwrap());
    fn_item.block.stmts.insert(0, syn::parse(quote!(let mut map = self.lock_map.lock().await;).into()).unwrap());
    let drop_statement = syn::parse(quote!(drop(lock_guard);).into()).unwrap();
    if fn_item.sig.output == syn::ReturnType::Default { // if there is no return, add the drop to last line
        fn_item.block.stmts.push(drop_statement);
    } else { // otherwise, add it to last line before it
        fn_item.block.stmts.insert(fn_item.block.stmts.len() - 1, drop_statement);
    }
    // Return the function
    use quote::ToTokens;
    item.into_token_stream().into()
}